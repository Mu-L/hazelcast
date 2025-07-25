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

import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class UpdateJobConfigOperation extends AsyncMasterOnlyJobOperation {
    private DeltaJobConfig deltaConfig;

    public UpdateJobConfigOperation() {
    }

    public UpdateJobConfigOperation(long jobId, DeltaJobConfig deltaConfig) {
        super(jobId);
        this.deltaConfig = deltaConfig;
    }

    @Override
    public CompletableFuture<JobConfig> doRun() {
        return getJobCoordinationService().updateJobConfig(jobId(), deltaConfig);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.UPDATE_JOB_CONFIG_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(deltaConfig);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        deltaConfig = in.readObject();
    }
}
