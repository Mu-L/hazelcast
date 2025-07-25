/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.operation;

import com.hazelcast.internal.cluster.impl.MasterNodeChangedException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

public class GetJobConfigOperation extends AbstractJobOperation implements AllowedDuringPassiveState, MasterAwareOperation {
    private boolean isLightJob;
    private JobConfig response;

    public GetJobConfigOperation() {
    }

    public GetJobConfigOperation(long jobId, boolean isLightJob) {
        super(jobId);
        this.isLightJob = isLightJob;
    }

    @Override
    public void beforeRun() throws Exception {
        if (isRequireMasterExecution() && !isMaster()) {
            throw new MasterNodeChangedException("This operation can only be executed on the master node.");
        }
        super.beforeRun();
    }

    private boolean isMaster() {
        return getNodeEngine().getClusterService().isMaster();
    }

    @Override
    public void run() {
        JetServiceBackend service = getJetServiceBackend();
        response = service.getJobConfig(jobId(), isLightJob);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_JOB_CONFIG_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(isLightJob);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        isLightJob = in.readBoolean();
    }

    @Override
    public boolean isRequireMasterExecution() {
        return !isLightJob;
    }
}
