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

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"

namespace doris {
class RowsetMetaPB;
class TSnapshotRequest;
struct RowsetId;
class StorageEngine;
class MemTrackerLimiter;

class LocalSnapshotLockGuard;

// A simple lock to protect the local snapshot path.
class LocalSnapshotLock {
    friend class LocalSnapshotLockGuard;

public:
    LocalSnapshotLock() = default;
    ~LocalSnapshotLock() = default;
    LocalSnapshotLock(const LocalSnapshotLock&) = delete;
    LocalSnapshotLock& operator=(const LocalSnapshotLock&) = delete;

    static LocalSnapshotLock& instance() {
        static LocalSnapshotLock instance;
        return instance;
    }

    // Acquire the lock for the specified path. It will block if the lock is already held by another.
    LocalSnapshotLockGuard acquire(const std::string& path);

private:
    void release(const std::string& path);

    class LocalSnapshotContext {
    public:
        bool _is_locked = false;
        size_t _waiting_count = 0;
        std::condition_variable _cv;

        LocalSnapshotContext() = default;
        LocalSnapshotContext(const LocalSnapshotContext&) = delete;
        LocalSnapshotContext& operator=(const LocalSnapshotContext&) = delete;
    };

    std::mutex _lock;
    std::unordered_map<std::string, LocalSnapshotContext> _local_snapshot_contexts;
};

class LocalSnapshotLockGuard {
public:
    LocalSnapshotLockGuard(std::string path) : _snapshot_path(std::move(path)) {}
    LocalSnapshotLockGuard(const LocalSnapshotLockGuard&) = delete;
    LocalSnapshotLockGuard& operator=(const LocalSnapshotLockGuard&) = delete;
    ~LocalSnapshotLockGuard() { LocalSnapshotLock::instance().release(_snapshot_path); }

private:
    std::string _snapshot_path;
};

class SnapshotManager {
public:
    SnapshotManager(StorageEngine& engine);
    ~SnapshotManager();

    /// Create a snapshot
    /// snapshot_path: out param, the dir of snapshot
    /// allow_incremental_clone: out param, true if it is an incremental clone
    Status make_snapshot(const TSnapshotRequest& request, std::string* snapshot_path,
                         bool* allow_incremental_clone);

    std::string static get_schema_hash_full_path(const TabletSharedPtr& ref_tablet,
                                                 const std::string& prefix);

    // @brief 释放snapshot
    // @param snapshot_path [in] 要被释放的snapshot的路径，只包含到ID
    Status release_snapshot(const std::string& snapshot_path);

    Result<std::vector<PendingRowsetGuard>> convert_rowset_ids(const std::string& clone_dir,
                                                               int64_t tablet_id,
                                                               int64_t replica_id, int64_t table_id,
                                                               int64_t partition_id,
                                                               int32_t schema_hash);

private:
    Status _calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s,
                                  std::string* out_path);

    std::string _get_header_full_path(const TabletSharedPtr& ref_tablet,
                                      const std::string& schema_hash_path) const;

    std::string _get_json_header_full_path(const TabletSharedPtr& ref_tablet,
                                           const std::string& schema_hash_path) const;

    Status _link_index_and_data_files(const std::string& header_path,
                                      const TabletSharedPtr& ref_tablet,
                                      const std::vector<RowsetSharedPtr>& consistent_rowsets);

    Status _create_snapshot_files(const TabletSharedPtr& ref_tablet,
                                  const TabletSharedPtr& target_tablet,
                                  const TSnapshotRequest& request, std::string* snapshot_path,
                                  bool* allow_incremental_clone);

    Status _prepare_snapshot_dir(const TabletSharedPtr& ref_tablet, std::string* snapshot_id_path);

    Status _rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const std::string& new_tablet_path,
                             TabletSchemaSPtr tablet_schema, const RowsetId& next_id,
                             RowsetMetaPB* new_rs_meta_pb);

    StorageEngine& _engine;
    std::atomic<uint64_t> _snapshot_base_id {0};

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
}; // SnapshotManager

} // namespace doris
