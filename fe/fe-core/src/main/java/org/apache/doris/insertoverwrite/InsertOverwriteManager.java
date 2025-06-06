// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.insertoverwrite;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.insertoverwrite.InsertOverwriteLog.InsertOverwriteOpType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertOverwriteManager extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteManager.class);

    private static final long CLEAN_INTERVAL_SECOND = 10;

    @SerializedName(value = "tasks")
    private Map<Long, InsertOverwriteTask> tasks = Maps.newConcurrentMap();

    // <txnId, <dbId, tableId>>
    // for iot auto detect tasks. a txn will make many task by different rpc
    @SerializedName(value = "taskGroups")
    private Map<Long, List<Long>> taskGroups = Maps.newConcurrentMap();
    // for one task group, there may be different requests about changing a partition to new.
    // but we only change one time and save the relations in partitionPairs. they're protected by taskLocks
    @SerializedName(value = "taskLocks")
    private Map<Long, ReentrantLock> taskLocks = Maps.newConcurrentMap();
    // <groupId, <oldPartId, newPartId>>. no need concern which task it belongs to.
    @SerializedName(value = "partitionPairs")
    private Map<Long, Map<Long, Long>> partitionPairs = Maps.newConcurrentMap();

    // TableId running insert overwrite
    // dbId ==> Set<tableId>
    private Map<Long, Set<Long>> runningTables = Maps.newHashMap();
    private ReentrantReadWriteLock runningLock = new ReentrantReadWriteLock(true);

    public InsertOverwriteManager() {
        super("InsertOverwriteDropDirtyPartitions", CLEAN_INTERVAL_SECOND * 1000);
    }

    /**
     * register insert overwrite task
     *
     * @param dbId
     * @param tableId
     * @param tempPartitionNames
     * @return taskId
     */
    public long registerTask(long dbId, long tableId, List<String> tempPartitionNames) {
        long taskId = Env.getCurrentEnv().getNextId();
        InsertOverwriteTask task = new InsertOverwriteTask(dbId, tableId,
                tempPartitionNames);
        tasks.put(taskId, task);
        Env.getCurrentEnv().getEditLog()
                .logInsertOverwrite(new InsertOverwriteLog(taskId, task, InsertOverwriteOpType.ADD));
        return taskId;
    }

    /**
     * register insert overwrite task group for auto detect partition.
     * it may have many tasks by FrontendService rpc deal.
     * all of them will be involved in one txn.(success or fallback)
     *
     * @return group id, like a transaction id.
     */
    public long registerTaskGroup() {
        long groupId = Env.getCurrentEnv().getNextId();
        taskGroups.put(groupId, new ArrayList<Long>());
        taskLocks.put(groupId, new ReentrantLock());
        partitionPairs.put(groupId, Maps.newConcurrentMap());
        return groupId;
    }

    /**
     * for iot auto detect. register task first. then put in group.
     */
    public void registerTaskInGroup(long groupId, long taskId) {
        LOG.info("register task " + taskId + " in group " + groupId);
        taskGroups.get(groupId).add(taskId);
    }

    /**
     * this func should in lock scope of getLock(groupId)
     *
     * @param newIds if have replaced, replace with new. otherwise itself.
     */
    public boolean tryReplacePartitionIds(long groupId, List<Long> oldPartitionIds, List<Long> newIds) {
        Map<Long, Long> relations = partitionPairs.get(groupId);
        boolean needReplace = false;
        for (int i = 0; i < oldPartitionIds.size(); i++) {
            long id = oldPartitionIds.get(i);
            if (relations.containsKey(id)) {
                // if we replaced it. then return new one.
                newIds.add(relations.get(id));
            } else {
                newIds.add(id);
                needReplace = true;
            }
        }
        return needReplace;
    }

    // this func should in lock scope of getLock(groupId)
    public void recordPartitionPairs(long groupId, List<Long> oldIds, List<Long> newIds) {
        Map<Long, Long> relations = partitionPairs.get(groupId);
        Preconditions.checkArgument(oldIds.size() == newIds.size());
        for (int i = 0; i < oldIds.size(); i++) {
            relations.put(oldIds.get(i), newIds.get(i));
            if (LOG.isDebugEnabled()) {
                LOG.debug("recorded partition pairs: [" + oldIds.get(i) + ", " + newIds.get(i) + "]");
            }
        }
    }

    // lock is a symbol of TaskGroup exist. if not, means already failed.
    public ReentrantLock getLock(long groupId) {
        return taskLocks.get(groupId);
    }

    // When goes into failure, some BE may still not know and send new request.
    // it will cause ConcurrentModification or NullPointer.
    public void taskGroupFail(long groupId) {
        LOG.info("insert overwrite auto detect partition task group [" + groupId + "] failed");
        ReentrantLock lock = getLock(groupId);
        lock.lock();
        try {
            // will rollback temp partitions in `taskFail`
            for (Long taskId : taskGroups.get(groupId)) {
                taskFail(taskId);
            }
            cleanTaskGroup(groupId);
        } finally {
            lock.unlock();
        }
    }

    // here we will make all raplacement of this group visiable. if someone fails, nothing happen.
    public void taskGroupSuccess(long groupId, OlapTable targetTable) throws DdlException {
        try {
            Map<Long, Long> relations = partitionPairs.get(groupId);
            ArrayList<String> oldNames = new ArrayList<>();
            ArrayList<String> newNames = new ArrayList<>();
            for (Entry<Long, Long> partitionPair : relations.entrySet()) {
                oldNames.add(targetTable.getPartition(partitionPair.getKey()).getName());
                newNames.add(targetTable.getPartition(partitionPair.getValue()).getName());
            }
            InsertOverwriteUtil.replacePartition(targetTable, oldNames, newNames);
        } catch (Exception e) {
            LOG.warn("insert overwrite task making replacement failed because " + e.getMessage()
                    + "all new partition will not be visible and will be recycled by partition GC.");
            throw e;
        }
        LOG.info("insert overwrite auto detect partition task group [" + groupId + "] succeed");
        for (Long taskId : taskGroups.get(groupId)) {
            Env.getCurrentEnv().getEditLog()
                    .logInsertOverwrite(new InsertOverwriteLog(taskId, tasks.get(taskId), InsertOverwriteOpType.ADD));
            taskSuccess(taskId);
        }
        cleanTaskGroup(groupId);
    }

    private void cleanTaskGroup(long groupId) {
        partitionPairs.remove(groupId);
        taskLocks.remove(groupId);
        taskGroups.remove(groupId);
    }

    /**
     * when insert overwrite fail, try drop temp partition
     *
     * @param taskId
     */
    public void taskFail(long taskId) {
        LOG.info("insert overwrite task [" + taskId + "] failed");
        boolean rollback = rollback(taskId);
        if (!rollback) {
            LOG.warn("roll back task [" + taskId + "] failed");
        }
        if (rollback) {
            removeTask(taskId);
        } else {
            cancelTask(taskId);
        }
    }

    /**
     * when insert overwrite success, drop task
     *
     * @param taskId
     */
    public void taskSuccess(long taskId) {
        LOG.info("insert overwrite task [" + taskId + "] succeed");
        removeTask(taskId);
    }

    /**
     * for transferToMaster, try drop all temp partitions
     */
    public void allTaskFail() {
        LOG.info("try drop all temp partitions when transferToMaster");
        HashMap<Long, InsertOverwriteTask> copyTasks = Maps.newHashMap(tasks);
        for (Entry<Long, InsertOverwriteTask> entry : copyTasks.entrySet()) {
            taskFail(entry.getKey());
        }
    }

    // cancel it. should try to remove them after.
    private void cancelTask(long taskId) {
        if (tasks.containsKey(taskId)) {
            LOG.info("cancel insert overwrite task: {}", tasks.get(taskId));
            tasks.get(taskId).setCancel(true);
            Env.getCurrentEnv().getEditLog()
                    .logInsertOverwrite(new InsertOverwriteLog(taskId, null, InsertOverwriteOpType.CANCEL));
        }
    }

    // task and partitions has been removed. it's safe to remove task.
    private void removeTask(long taskId) {
        if (tasks.containsKey(taskId)) {
            LOG.info("remove insert overwrite task: {}", tasks.get(taskId));
            tasks.remove(taskId);
            Env.getCurrentEnv().getEditLog()
                    .logInsertOverwrite(new InsertOverwriteLog(taskId, null, InsertOverwriteOpType.DROP));
        }
    }

    /**
     * drop temp partitions
     *
     * @param taskId
     * @return if success
     */
    private boolean rollback(long taskId) {
        InsertOverwriteTask task = tasks.get(taskId);
        OlapTable olapTable;
        try {
            olapTable = task.getTable();
        } catch (DdlException e) {
            LOG.warn("can not get table, task: {}, reason: {}", task, e.getMessage());
            return true;
        }
        return InsertOverwriteUtil.dropPartitions(olapTable, task.getTempPartitionNames());
    }

    /**
     * If the current table id has a running insert overwrite, throw an exception.
     * If not, record it in runningTables
     *
     * @param db Run the db for insert overwrite
     * @param table Run the table for insert overwrite
     */
    public void recordRunningTableOrException(DatabaseIf db, TableIf table) {
        // The logic of OlapTable executing insert overwrite is to create temporary partitions,
        // replace partitions, etc.
        // If executed in parallel, it may cause problems such as not being able to find temporary partitions.
        // But in terms of external table, we don't care the internal logic of execution,
        // so there's no need to keep records
        if (!(table instanceof MTMV)) {
            return;
        }
        long dbId = db.getId();
        long tableId = table.getId();
        runningLock.writeLock().lock();
        try {
            if (runningTables.containsKey(dbId) && runningTables.get(dbId).contains(tableId)) {
                throw new AnalysisException(
                        String.format("Not allowed running Insert Overwrite on same table: %s.%s", db.getFullName(),
                                table.getName()));
            }
            if (runningTables.containsKey(dbId)) {
                runningTables.get(dbId).add(tableId);
            } else {
                runningTables.put(dbId, Sets.newHashSet(tableId));
            }
        } finally {
            runningLock.writeLock().unlock();
        }
    }

    /**
     * Remove from running records
     *
     * @param dbId Run the dbId for insert overwrite
     * @param tableId Run the tableId for insert overwrite
     */
    public void dropRunningRecord(long dbId, long tableId) {
        runningLock.writeLock().lock();
        try {
            if (runningTables.containsKey(dbId) && runningTables.get(dbId).contains(tableId)) {
                runningTables.get(dbId).remove(tableId);
                if (runningTables.get(dbId).size() == 0) {
                    runningTables.remove(dbId);
                }
            }
        } finally {
            runningLock.writeLock().unlock();
        }
    }

    /**
     * replay logs
     *
     * @param insertOverwriteLog
     */
    public void replayInsertOverwriteLog(InsertOverwriteLog insertOverwriteLog) {
        switch (insertOverwriteLog.getOpType()) {
            case ADD:
                tasks.put(insertOverwriteLog.getTaskId(), insertOverwriteLog.getTask());
                break;
            case DROP:
                tasks.remove(insertOverwriteLog.getTaskId());
                break;
            case CANCEL:
                InsertOverwriteTask task = tasks.get(insertOverwriteLog.getTaskId());
                if (task != null) {
                    task.setCancel(true);
                }
                break;
            default:
                LOG.warn("error insertOverwriteLog: {}", insertOverwriteLog.toString());
        }
    }

    /**
     * Regularly drop partitions that have failed dropped
     */
    @Override
    protected void runAfterCatalogReady() {
        LOG.info("start clean insert overwrite temp partitions");
        HashMap<Long, InsertOverwriteTask> copyTasks = Maps.newHashMap(tasks);
        for (Entry<Long, InsertOverwriteTask> entry : copyTasks.entrySet()) {
            if (entry.getValue().isCancel()) {
                boolean rollback = rollback(entry.getKey());
                if (rollback) {
                    removeTask(entry.getKey());
                }
            }
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static InsertOverwriteManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InsertOverwriteManager.class);
    }
}
