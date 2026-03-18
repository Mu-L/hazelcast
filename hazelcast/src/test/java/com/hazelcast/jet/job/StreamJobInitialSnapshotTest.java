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
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.MapInterceptorAdaptor;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamJobInitialSnapshotTest extends AbstractJobInitialSnapshotTest {

    @Test
    public void streamInitialSnapshotDone() {
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        testJob();
        if (regularSnapshotEnabled) {
            assertSnapshotExists(job);
        } else {
            assertSnapshotId(job, 0);
        }
    }

    @Test
    public void cancelImmediateAfterStart() {
        BlockingSnapshot.blockSnapshot();
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.STARTING);
        assertThatNoException().isThrownBy(job::cancel);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
    }

    @Test
    public void initialSnapshotFailedPhase1_suspendOnFailureFalse() {
        FailingSnapshot.enableFailingPhase1();
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
    }

    @Test
    public void initialSnapshotFailedSilentPhase1_suspendOnFailureFalse() {
        BlockingSnapshot.blockSnapshot();

        createStreamPipline();

        var job = start(getJobConfig(false));
        var snapshotMap = JobRepository.snapshotDataMapName(job.getId(), 0);
        hz().getMap(snapshotMap).addInterceptor(new FailingInterceptor());
        BlockingSnapshot.unblockSnapshot();
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
    }

    @Test
    public void initialSnapshotFailedSilentPhase1_suspendOnFailureTrue() {
        BlockingSnapshot.blockSnapshot();

        createStreamPipline();

        var job = start(getJobConfig(true));
        var snapshotMap = JobRepository.snapshotDataMapName(job.getId(), 0);
        hz().getMap(snapshotMap).addInterceptor(new FailingInterceptor());
        BlockingSnapshot.unblockSnapshot();
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
    }

    @Test
    public void initialSnapshotFailedPhase2_suspendOnFailureFalse() {
        FailingSnapshot.enableFailingPhase2();
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.FAILED);
    }

    @Test
    public void initialSnapshotFailedPhase1_suspendOnFailureTrue() {
        FailingSnapshot.enableFailingPhase1();
        createStreamPipline();
        var job = start(getJobConfig(true));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.SUSPENDED);
        assertNoSnapshotExists(job);

        FailingSnapshot.disableFailing();
        job.resume();

        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        if (regularSnapshotEnabled) {
            assertSnapshotExists(job);
        } else {
            // Snapshot 0 failed.
            // Next snapshot number is 1.
            assertSnapshotId(job, 1);
        }
    }

    @Test
    public void initialSnapshotFailedPhase2_suspendOnFailureTrue() {
        FailingSnapshot.enableFailingPhase2();
        createStreamPipline();
        var job = start(getJobConfig(true));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.SUSPENDED);
        assertSnapshotId(job, 0);

        FailingSnapshot.disableFailing();
        job.resume();

        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        testJob();
    }

    @Test
    public void restartJob_suspendOnFailureTrue() {
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        if (regularSnapshotEnabled) {
            assertSnapshotExists(job);
        } else {
            assertSnapshotId(job, 0);
        }

        job.restart();

        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        if (!regularSnapshotEnabled) {
            assertSnapshotId(job, 1);
        }
        testJob();
    }

    @Test
    public void regularSnapshotScheduledAfterInitialSnapshot() {
        var snapshotInterval = Duration.ofHours(3);
        createStreamPipline();
        JobConfig config = getJobConfig(true);
        config.setSnapshotIntervalMillis(snapshotInterval.toMillis());
        var job = start(config);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        HazelcastTestSupport.assertTrueAllTheTime(
            () -> assertSnapshotId(job, 0), ASSERT_TRUE_ALL_THE_TIME_TIMEOUT.toSeconds()
        );
    }

    @Test
    public void noProcessingGuarantee_then_noInitialSnapshot() {
        createStreamPipline();
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.NONE);
        config.setRequireSnapshotBeforeProcessing(true);
        var job = start(config);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        assertNoSnapshotExists(job);
    }

    @Test
    public void runningStateOnlyAfterSnapshotIsDone() {
        BlockingSnapshot.blockSnapshot();
        createStreamPipline();
        var job = start(getJobConfig(false));
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.STARTING);
        HazelcastTestSupport.assertTrueAllTheTime(
            () -> Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.STARTING), ASSERT_TRUE_ALL_THE_TIME_TIMEOUT.toSeconds()
        );
        BlockingSnapshot.unblockSnapshot();
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        testJob();
    }

    private void createStreamPipline() {
        p.readFrom(Sources.mapJournal(srcName, JournalInitialPosition.START_FROM_CURRENT))
            .withoutTimestamps()
            .map(Map.Entry::getKey)
            .customTransform("blockingSnapshot", BlockingSnapshot::new)
            .customTransform("failingSnapshot", FailingSnapshot::new)
            .writeTo(Sinks.list(sinkList));
    }


    public static class FailingInterceptor extends MapInterceptorAdaptor implements Serializable {
        public Object interceptPut(Object oldValue, Object newValue) {
            throw new RuntimeException("Failed to update snapshot map!");
        }
    }
}
