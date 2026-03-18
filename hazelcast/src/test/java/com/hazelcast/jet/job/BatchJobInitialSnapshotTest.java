/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.job;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobAssertions;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;


@Category({QuickTest.class, ParallelJVMTest.class})
public class BatchJobInitialSnapshotTest extends AbstractJobInitialSnapshotTest {

    private static final int SIZE = 100;
    @Test
    public void streamInitialSnapshotDone() {
        createPipline();
        fillMap(SIZE);
        var job = execute(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.COMPLETED);
        assertThat(sinkList.size()).isEqualTo(SIZE);
    }

    // no initial snapshot but try to complete
    @Test
    public void cancelImmediateAfterStart() {
        BlockingSnapshot.blockSnapshot();
        createPipline();
        fillMap(SIZE);
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.STARTING);
        assertThatNoException().isThrownBy(job::cancel);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
        assertNothingProcessed();
    }


    @Test
    public void initialSnapshotFailed_suspendOnFailureFalse() {
        FailingSnapshot.enableFailingPhase1();
        createPipline();
        fillMap(SIZE);
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
        assertNothingProcessed();
    }

    @Test
    public void initialSnapshotFailed_suspendOnFailureTrue() {
        FailingSnapshot.enableFailingPhase1();
        createPipline();
        fillMap(SIZE);

        var job = start(getJobConfig(true));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.SUSPENDED);
        assertNoSnapshotExists(job);

        FailingSnapshot.disableFailing();
        job.resume();

        job.join();

        assertThat(sinkList.size()).isEqualTo(SIZE);
    }

    @Test
    public void restartJob() {
        BlockingStreamProcessor.block();
        createPipline();
        fillMap(SIZE);
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        if (regularSnapshotEnabled) {
            assertSnapshotExists(job);
        } else {
            assertSnapshotId(job, 0);
        }

        job.restart();

        BlockingStreamProcessor.unblock();
        job.join();

        assertThat(sinkList.size()).isGreaterThanOrEqualTo(SIZE);
    }

    @Test
    public void noProcessingGuarantee_then_noInitialSnapshot() {
        BlockingStreamProcessor.block();
        fillMap(SIZE);
        createPipline();
        var config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.NONE);
        config.setRequireSnapshotBeforeProcessing(true);
        var job = start(config);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        assertNoSnapshotExists(job);
        BlockingStreamProcessor.unblock();
        job.join();
    }

    @Test
    public void runningStateOnlyAfterSnapshotIsDone() {
        BlockingSnapshot.blockSnapshot();
        fillMap(SIZE);
        createPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.STARTING);
        assertTrueAllTheTime(
            () -> assertThat(job.getStatus()).isEqualTo(JobStatus.STARTING), ASSERT_TRUE_ALL_THE_TIME_TIMEOUT.toSeconds()
        );
        BlockingSnapshot.unblockSnapshot();
        job.join();
        assertThat(sinkList.size()).isEqualTo(SIZE);
    }

    private void createPipline() {
        p.readFrom(Sources.map(srcName))
            .map(Map.Entry::getKey)
            .customTransform("blockingSnapshot", BlockingSnapshot::new)
            .customTransform("failingSnapshot", FailingSnapshot::new)
            .customTransform("blockStream", BlockingStreamProcessor::new)
            .writeTo(Sinks.list(sinkList));
    }

    private void assertNothingProcessed() {
        assertThat(sinkList.size()).isEqualTo(0);
    }
}
