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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;

import org.apache.commons.lang3.StringUtils;

/**
 * drop workload group command
 */
public class DropWorkloadGroupCommand extends DropCommand {
    private final boolean ifExists;
    private final String workloadGroupName;
    private String computeGroup;

    /**
     * constructor
     */
    public DropWorkloadGroupCommand(String computeGroupName, String workloadGroupName, boolean ifExists) {
        super(PlanType.DROP_WORKLOAD_GROUP_NAME);

        this.workloadGroupName = workloadGroupName;
        this.computeGroup = computeGroupName;
        this.ifExists = ifExists;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (StringUtils.isEmpty(computeGroup)) {
            computeGroup = Config.isCloudMode() ? Tag.VALUE_DEFAULT_COMPUTE_GROUP_NAME : Tag.DEFAULT_BACKEND_TAG.value;
        }

        if (Config.isCloudMode()) {
            String originStr = computeGroup;
            computeGroup = ((CloudSystemInfoService) Env.getCurrentEnv().getClusterInfo()).getCloudClusterIdByName(
                    computeGroup);
            if (StringUtils.isEmpty(computeGroup)) {
                throw new UserException("Can not find compute group " + originStr + ".");
            }
        }

        Env.getCurrentEnv().getWorkloadGroupMgr().dropWorkloadGroup(computeGroup, workloadGroupName, ifExists);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropWorkloadGroupCommand(this, context);
    }
}
