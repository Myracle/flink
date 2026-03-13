/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.sampling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.DataSampleResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexDataSampleHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDataSampleParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for data sampling E2E: MiniCluster + streaming job + REST endpoint. */
class DataSamplingITCase {

    private static final int NUM_SLOTS = 4;
    private static final int MAP_PARALLELISM = 2;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration(true))
                            .setNumberSlotsPerTaskManager(NUM_SLOTS)
                            .build());

    private static Configuration createConfiguration(boolean enableDataSampling) {
        final Configuration config = new Configuration();
        config.set(RestOptions.ENABLE_DATA_SAMPLING, enableDataSampling);
        config.set(RestOptions.DATA_SAMPLING_SAMPLING_WINDOW, Duration.ofSeconds(1));
        config.set(RestOptions.DATA_SAMPLING_TIMEOUT, Duration.ofSeconds(10));
        config.set(RestOptions.DATA_SAMPLING_REFRESH_INTERVAL, Duration.ofSeconds(30));
        return config;
    }

    @Test
    @Timeout(60)
    void testDataSamplingEndToEnd(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        // Build streaming job: Source -> Map -> Sink (disable chaining for separate vertices)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();

        DataStream<Long> source =
                env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "Sequence Source");

        source.map(x -> x)
                .setParallelism(MAP_PARALLELISM)
                .name("Map")
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // Submit job
        miniCluster.submitJob(jobGraph).get();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final URI restAddress = miniCluster.getRestAddress().join();
            final RestClient restClient = new RestClient(new Configuration(), executor);

            // Wait for all tasks to be RUNNING and get vertex details
            final JobDetailsInfo jobDetails =
                    waitForAllTasksRunning(restClient, restAddress, jobId);

            // Pick the Map vertex deterministically
            final JobVertexID mapVertexId = findVertexId(jobDetails, "Map");
            assertThat(mapVertexId).isNotNull();

            // Poll data-sample endpoint until we get a non-PENDING response
            final AtomicReference<DataSampleResponseBody> responseRef = new AtomicReference<>();
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        DataSampleResponseBody resp =
                                requestDataSample(
                                        restClient, restAddress, jobId, mapVertexId, null, null);
                        if (!"PENDING".equals(resp.getStatus())) {
                            responseRef.set(resp);
                            return true;
                        }
                        return false;
                    });

            DataSampleResponseBody response = responseRef.get();

            // Verify response status and top-level fields
            assertThat(response.getStatus()).isEqualTo("COMPLETE");
            assertThat(response.getEndTimestamp()).isGreaterThan(0);
            assertThat(response.getRoundId()).isGreaterThanOrEqualTo(0);
            assertThat(response.getTotalRecordCount()).isGreaterThan(0);
            assertThat(response.isTotalTruncated()).isFalse();
            assertThat(response.getErrorCode()).isNull();
            assertThat(response.getFailedSubtasks()).isNullOrEmpty();
            assertThat(response.getDroppedByContention()).isGreaterThanOrEqualTo(0);
            assertThat(response.getDroppedByRateLimit()).isGreaterThanOrEqualTo(0);

            // Verify multi-subtask: Map has parallelism=2, expect 2 subtask samples
            assertThat(response.getSamples()).isNotNull();
            assertThat(response.getSamples()).hasSize(MAP_PARALLELISM);

            Set<Integer> subtaskIndices =
                    response.getSamples().stream()
                            .map(DataSampleResponseBody.SubtaskSampleResponseBody::getSubtaskIndex)
                            .collect(Collectors.toSet());
            assertThat(subtaskIndices).containsExactlyInAnyOrder(0, 1);

            // Check samples have correct structure and data content
            for (DataSampleResponseBody.SubtaskSampleResponseBody sample : response.getSamples()) {
                assertThat(sample.getSubtaskIndex()).isGreaterThanOrEqualTo(0);
                assertThat(sample.getRecords()).isNotEmpty();
                for (DataSampleResponseBody.SampledRecordResponseBody record :
                        sample.getRecords()) {
                    assertThat(record.getSampleTimestamp()).isGreaterThan(0);
                    // NumberSequenceSource produces Long values
                    assertThat(record.getDataType()).contains("Long");
                    assertThat(record.getData()).matches("\\d+");
                    assertThat(record.isTruncated()).isFalse();
                    assertThat(record.getSideOutputName()).isNull();
                }
            }

            restClient.close();
        } finally {
            executor.shutdown();
            miniCluster.cancelJob(jobId).get();
        }
    }

    @Test
    @Timeout(60)
    void testDataSamplingDisabled() throws Exception {
        final Configuration config = createConfiguration(false);

        final MiniClusterConfiguration miniClusterConfig =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(NUM_SLOTS)
                        .setConfiguration(config)
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(miniClusterConfig)) {
            miniCluster.start();

            // Build and submit a simple job (disable chaining for separate vertices)
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.disableOperatorChaining();
            env.fromSource(
                            new NumberSequenceSource(0, Long.MAX_VALUE),
                            WatermarkStrategy.noWatermarks(),
                            "Source")
                    .map(x -> x)
                    .name("Map")
                    .sinkTo(new DiscardingSink<>());

            final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            final JobID jobId = jobGraph.getJobID();

            miniCluster.submitJob(jobGraph).get();

            final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            try {
                final URI restAddress = miniCluster.getRestAddress().join();
                final RestClient restClient = new RestClient(new Configuration(), executor);

                // Wait for tasks to be RUNNING and find Map vertex
                final JobDetailsInfo jobDetails =
                        waitForAllTasksRunning(restClient, restAddress, jobId);
                final JobVertexID mapVertexId = findVertexId(jobDetails, "Map");
                assertThat(mapVertexId).isNotNull();

                // Call data-sample endpoint
                DataSampleResponseBody response =
                        requestDataSample(restClient, restAddress, jobId, mapVertexId, null, null);

                assertThat(response.getStatus()).isEqualTo("DISABLED");
                assertThat(response.getSamples()).isNull();
                assertThat(response.getErrorCode()).isNull();
                assertThat(response.getTotalRecordCount()).isEqualTo(0);

                restClient.close();
            } finally {
                miniCluster.cancelJob(jobId).get();
                executor.shutdown();
            }
        }
    }

    @Test
    @Timeout(60)
    void testSubtaskFilterQueryParameter(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();

        env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "Sequence Source")
                .map(x -> x)
                .setParallelism(MAP_PARALLELISM)
                .name("Map")
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final JobID jobId = jobGraph.getJobID();

        miniCluster.submitJob(jobGraph).get();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final URI restAddress = miniCluster.getRestAddress().join();
            final RestClient restClient = new RestClient(new Configuration(), executor);

            final JobDetailsInfo jobDetails =
                    waitForAllTasksRunning(restClient, restAddress, jobId);
            final JobVertexID mapVertexId = findVertexId(jobDetails, "Map");
            assertThat(mapVertexId).isNotNull();

            // Wait until data is available
            final AtomicReference<DataSampleResponseBody> responseRef = new AtomicReference<>();
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        DataSampleResponseBody resp =
                                requestDataSample(
                                        restClient, restAddress, jobId, mapVertexId, 0, null);
                        if (!"PENDING".equals(resp.getStatus())) {
                            responseRef.set(resp);
                            return true;
                        }
                        return false;
                    });

            DataSampleResponseBody response = responseRef.get();

            // Only subtask 0 should be returned
            assertThat(response.getStatus()).isEqualTo("COMPLETE");
            assertThat(response.getSamples()).hasSize(1);
            assertThat(response.getSamples().get(0).getSubtaskIndex()).isEqualTo(0);

            restClient.close();
        } finally {
            executor.shutdown();
            miniCluster.cancelJob(jobId).get();
        }
    }

    @Test
    @Timeout(60)
    void testMaxRecordsQueryParameter(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();

        env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "Sequence Source")
                .map(x -> x)
                .setParallelism(MAP_PARALLELISM)
                .name("Map")
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final JobID jobId = jobGraph.getJobID();

        miniCluster.submitJob(jobGraph).get();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final URI restAddress = miniCluster.getRestAddress().join();
            final RestClient restClient = new RestClient(new Configuration(), executor);

            final JobDetailsInfo jobDetails =
                    waitForAllTasksRunning(restClient, restAddress, jobId);
            final JobVertexID mapVertexId = findVertexId(jobDetails, "Map");
            assertThat(mapVertexId).isNotNull();

            // Wait until data is available, then request with maxRecords=1
            final AtomicReference<DataSampleResponseBody> responseRef = new AtomicReference<>();
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        DataSampleResponseBody resp =
                                requestDataSample(
                                        restClient, restAddress, jobId, mapVertexId, null, 1);
                        if (!"PENDING".equals(resp.getStatus())) {
                            responseRef.set(resp);
                            return true;
                        }
                        return false;
                    });

            DataSampleResponseBody response = responseRef.get();

            assertThat(response.getStatus()).isEqualTo("COMPLETE");
            assertThat(response.getSamples()).isNotNull().isNotEmpty();

            // Each subtask should have at most 1 record
            for (DataSampleResponseBody.SubtaskSampleResponseBody sample : response.getSamples()) {
                assertThat(sample.getRecords()).hasSizeLessThanOrEqualTo(1);
            }

            restClient.close();
        } finally {
            executor.shutdown();
            miniCluster.cancelJob(jobId).get();
        }
    }

    @Test
    @Timeout(60)
    void testInvalidVertexId(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();

        env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "Sequence Source")
                .map(x -> x)
                .name("Map")
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final JobID jobId = jobGraph.getJobID();

        miniCluster.submitJob(jobGraph).get();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final URI restAddress = miniCluster.getRestAddress().join();
            final RestClient restClient = new RestClient(new Configuration(), executor);

            waitForAllTasksRunning(restClient, restAddress, jobId);

            // Request with a non-existent vertex ID
            final JobVertexID fakeVertexId = new JobVertexID();

            assertThatThrownBy(
                            () ->
                                    requestDataSample(
                                            restClient,
                                            restAddress,
                                            jobId,
                                            fakeVertexId,
                                            null,
                                            null))
                    .isInstanceOf(ExecutionException.class);

            restClient.close();
        } finally {
            executor.shutdown();
            miniCluster.cancelJob(jobId).get();
        }
    }

    // ---- Helpers ----

    /** Waits until all tasks are RUNNING and returns the details info for further vertex lookup. */
    private static JobDetailsInfo waitForAllTasksRunning(
            RestClient restClient, URI restAddress, JobID jobId) throws Exception {
        final AtomicReference<JobDetailsInfo> detailsRef = new AtomicReference<>();
        CommonTestUtils.waitUntilCondition(
                () -> {
                    JobMessageParameters params = new JobMessageParameters();
                    params.jobPathParameter.resolve(jobId);
                    JobDetailsInfo details =
                            restClient
                                    .sendRequest(
                                            restAddress.getHost(),
                                            restAddress.getPort(),
                                            JobDetailsHeaders.getInstance(),
                                            params,
                                            EmptyRequestBody.getInstance())
                                    .get();
                    boolean allRunning =
                            !details.getJobVertexInfos().isEmpty()
                                    && details.getJobVertexInfos().stream()
                                            .allMatch(
                                                    v ->
                                                            v.getExecutionState()
                                                                    == ExecutionState.RUNNING);
                    if (allRunning) {
                        detailsRef.set(details);
                    }
                    return allRunning;
                });
        return detailsRef.get();
    }

    /** Finds a vertex by name substring from job details, returns null if not found. */
    private static JobVertexID findVertexId(JobDetailsInfo details, String nameContains) {
        return details.getJobVertexInfos().stream()
                .filter(v -> v.getName().contains(nameContains))
                .map(JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID)
                .findFirst()
                .orElse(null);
    }

    private static DataSampleResponseBody requestDataSample(
            RestClient restClient,
            URI restAddress,
            JobID jobId,
            JobVertexID vertexId,
            @Nullable Integer subtaskIndex,
            @Nullable Integer maxRecords)
            throws Exception {
        JobVertexDataSampleParameters dsParams = new JobVertexDataSampleParameters();
        dsParams.jobPathParameter.resolve(jobId);
        dsParams.jobVertexIdPathParameter.resolve(vertexId);
        if (subtaskIndex != null) {
            dsParams.subtaskIndexQueryParameter.resolve(Collections.singletonList(subtaskIndex));
        }
        if (maxRecords != null) {
            dsParams.maxRecordsQueryParameter.resolve(Collections.singletonList(maxRecords));
        }

        return restClient
                .sendRequest(
                        restAddress.getHost(),
                        restAddress.getPort(),
                        JobVertexDataSampleHeaders.getInstance(),
                        dsParams,
                        EmptyRequestBody.getInstance())
                .get();
    }
}
