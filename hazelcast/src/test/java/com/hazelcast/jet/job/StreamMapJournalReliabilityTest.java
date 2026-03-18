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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobAssertions;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class StreamMapJournalReliabilityTest extends HazelcastTestSupport {

    private static final int CLUSTER_SIZE = 2;
    private static final String INPUT_MAP = "inputMap";
    private static final String OUTPUT = "output";
    private static final int LARGE_JOURNAL_CAPACITY = 1000000;

    private HazelcastInstance instance;
    private HazelcastInstance[] nodes;

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig(INPUT_MAP)
            .getEventJournalConfig()
            .setCapacity(LARGE_JOURNAL_CAPACITY)
            .setEnabled(true);
        return config;
    }

    @Before
    public void before() {
        nodes = createHazelcastInstances(getConfig(), CLUSTER_SIZE);
        instance = nodes[0];
    }

    @Test
    public void whenJobRestartAfterSnapshot_then_noDataLoss() {
        IMap<String, Integer> inputMap = instance.getMap(INPUT_MAP);
        int expectedSize = 1000;

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        Job job = runJob(jobConfig);

        inputMap.put("key0", 0);
        assertTrueEventually(() -> assertThat(instance.getList(OUTPUT)).isNotEmpty());

        job.suspend();
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.SUSPENDED);
        assertSnapshotExists(job);

        var expectedKeys = populateMap(expectedSize);

        job.resume();
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        var result = instance.getList(OUTPUT);

        assertTrueEventually(() ->
            assertEquals("Unexpected output size", expectedSize + 1, result.size())
        );
        expectedKeys.forEach(k -> assertTrue("Missing key: " + k, result.contains(k)));
    }

    @Test
    public void whenJobRestartWithInitialSnapshot_then_startProcessingFromInitialSnapshot() {
        IMap<String, Integer> inputMap = instance.getMap(INPUT_MAP);
        int expectedSize = 1000;

        inputMap.put("beforeJob", 0);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        jobConfig.setSnapshotIntervalMillis(Duration.ofHours(12).toMillis());
        jobConfig.setRequireSnapshotBeforeProcessing(true);

        Job job = runJob(jobConfig);
        assertSnapshotExists(job);

        populateMap(expectedSize);
        var result = instance.getList(OUTPUT);
        assertTrueEventually(() -> assertEquals(expectedSize, result.size()));

        nodes[1].getLifecycleService().terminate();

        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        assertTrueEventually(() ->
            assertEquals("expect all data to be processed twice.", 2 * expectedSize, result.size())
        );
    }

    @Test
    public void whenJobRestartWithoutInitialSnapshot_then_startProcessingFromCurrent() {
        IMap<String, Integer> inputMap = instance.getMap(INPUT_MAP);
        int expectedSize = 1000;

        inputMap.put("beforeJob", 0);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        jobConfig.setSnapshotIntervalMillis(Duration.ofHours(12).toMillis());

        Job job = runJob(jobConfig);
        assertNoSnapshotExists(job);

        populateMap(expectedSize);
        var result = instance.getList(OUTPUT);
        assertTrueEventually(() -> assertEquals(expectedSize, result.size()));

        nodes[1].getLifecycleService().terminate();

        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        assertTrueEventually(() ->
            assertEquals("expect all data to be processed once.", expectedSize, result.size())
        );
    }

    private Job runJob(JobConfig jobConfig) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(INPUT_MAP, JournalInitialPosition.START_FROM_CURRENT))
            .withoutTimestamps()
            .map(Map.Entry::getKey)
            .writeTo(Sinks.list(OUTPUT));

        Job job = instance.getJet().newJob(p, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        return job;
    }

    void assertSnapshotExists(Job job) {
        assertThat(getSnapshot(job)).isNotEqualTo(NO_SNAPSHOT);
    }

    void assertNoSnapshotExists(Job job) {
        assertThat(getSnapshot(job)).isEqualTo(NO_SNAPSHOT);
    }

    long getSnapshot(Job job) {
        IMap<Long, JobExecutionRecord> executions =
            instance.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);

        JobExecutionRecord execution = executions.get(job.getId());
        assertThat(execution).isNotNull();
        return execution.snapshotId();
    }

    List<String> populateMap(int size) {
        var inputMap = instance.getMap(INPUT_MAP);
        return range(0, size).mapToObj(i -> {
            var key = "key" + i;
            inputMap.put(key, i);
            return key;
        }).toList();
    }
}
