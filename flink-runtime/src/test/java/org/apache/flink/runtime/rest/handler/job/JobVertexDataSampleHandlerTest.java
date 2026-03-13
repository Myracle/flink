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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.DataSampleResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDataSampleParameters;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MaxRecordsQueryParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexQueryParameter;
import org.apache.flink.runtime.sampling.SampleStatus;
import org.apache.flink.runtime.sampling.SampledRecord;
import org.apache.flink.runtime.sampling.SubtaskDataSample;
import org.apache.flink.runtime.sampling.VertexDataSampleStats;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JobVertexDataSampleHandler}. */
class JobVertexDataSampleHandlerTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final Duration STATS_REFRESH_INTERVAL = Duration.ofSeconds(10);

    @Test
    void testHandleRunningVertex() throws Exception {
        VertexDataSampleStats stats = createSampleStats(SampleStatus.COMPLETE, 2);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.RUNNING, 2);
        HandlerRequest<EmptyRequestBody> request = createRequest(null, null);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.getStatus()).isEqualTo("COMPLETE");
        assertThat(response.getSamples()).hasSize(2);
    }

    @Test
    void testHandleFinishedVertex() throws Exception {
        VertexDataSampleStats stats = createSampleStats(SampleStatus.COMPLETE, 2);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.FINISHED, 2);
        HandlerRequest<EmptyRequestBody> request = createRequest(null, null);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.getStatus()).isEqualTo("FAILED");
        assertThat(response.getErrorCode()).isEqualTo("TASK_TERMINATED");
    }

    @Test
    void testHandleMixedSubtasks() throws Exception {
        VertexDataSampleStats stats = createSampleStats(SampleStatus.COMPLETE, 2);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex =
                new ArchivedExecutionJobVertex(
                        new ArchivedExecutionVertex[] {
                            generateExecutionVertex(0, ExecutionState.FINISHED),
                            generateExecutionVertex(1, ExecutionState.RUNNING)
                        },
                        JOB_VERTEX_ID,
                        "test",
                        2,
                        2,
                        new SlotSharingGroup(),
                        ResourceProfile.UNKNOWN,
                        new StringifiedAccumulatorResult[0]);

        // Query the finished subtask → terminated
        HandlerRequest<EmptyRequestBody> request = createRequest(0, null);
        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);
        assertThat(response.getStatus()).isEqualTo("FAILED");

        // Query the running subtask → uses tracker stats
        request = createRequest(1, null);
        response = handler.handleRequest(request, jobVertex);
        assertThat(response.getStatus()).isEqualTo("COMPLETE");
    }

    @Test
    void testHandleWaiting() throws Exception {
        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(null));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.RUNNING, 1);
        HandlerRequest<EmptyRequestBody> request = createRequest(null, null);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.getStatus()).isEqualTo("PENDING");
    }

    @Test
    void testSubtaskFilter() throws Exception {
        VertexDataSampleStats stats = createSampleStats(SampleStatus.COMPLETE, 3);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.RUNNING, 3);
        HandlerRequest<EmptyRequestBody> request = createRequest(1, null);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.getSamples()).hasSize(1);
        assertThat(response.getSamples().get(0).getSubtaskIndex()).isEqualTo(1);
    }

    @Test
    void testMaxRecordsFilter() throws Exception {
        long now = System.currentTimeMillis();
        List<SampledRecord> manyRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            manyRecords.add(new SampledRecord(now, null, "r" + i, "String", false, null));
        }
        SubtaskDataSample subtask = new SubtaskDataSample(0, manyRecords, 10, false);

        VertexDataSampleStats stats =
                new VertexDataSampleStats(
                        SampleStatus.COMPLETE,
                        now,
                        1,
                        Collections.singletonList(subtask),
                        10,
                        false,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.RUNNING, 1);
        HandlerRequest<EmptyRequestBody> request = createRequest(null, 3);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.getSamples()).hasSize(1);
        assertThat(response.getSamples().get(0).getRecords()).hasSize(3);
    }

    @Test
    void testStalenessDetection() throws Exception {
        long staleTime = System.currentTimeMillis() - STATS_REFRESH_INTERVAL.toMillis() - 1000;

        VertexDataSampleStats stats =
                new VertexDataSampleStats(
                        SampleStatus.COMPLETE,
                        staleTime,
                        1,
                        Collections.singletonList(
                                new SubtaskDataSample(
                                        0,
                                        Collections.singletonList(
                                                new SampledRecord(
                                                        staleTime, null, "old", "String", false,
                                                        null)),
                                        1,
                                        false)),
                        1,
                        false,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null);

        JobVertexDataSampleHandler handler = createHandler(new TestDataSampleTracker(stats));

        ArchivedExecutionJobVertex jobVertex = createJobVertex(ExecutionState.RUNNING, 1);
        HandlerRequest<EmptyRequestBody> request = createRequest(null, null);

        DataSampleResponseBody response = handler.handleRequest(request, jobVertex);

        assertThat(response.isStale()).isTrue();
    }

    // ---- Helpers ----

    private static VertexDataSampleStats createSampleStats(SampleStatus status, int numSubtasks) {
        long now = System.currentTimeMillis();
        List<SubtaskDataSample> samples = new ArrayList<>();
        for (int i = 0; i < numSubtasks; i++) {
            samples.add(
                    new SubtaskDataSample(
                            i,
                            Collections.singletonList(
                                    new SampledRecord(
                                            now, null, "data-" + i, "String", false, null)),
                            1,
                            false));
        }
        return new VertexDataSampleStats(
                status,
                now,
                1,
                samples,
                numSubtasks,
                false,
                0,
                0,
                Collections.emptyList(),
                null,
                null);
    }

    private static JobVertexDataSampleHandler createHandler(
            VertexStatsTracker<VertexDataSampleStats> tracker) {
        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(new Configuration());
        return new JobVertexDataSampleHandler(
                () -> null,
                Duration.ofMillis(100L),
                Collections.emptyMap(),
                new DefaultExecutionGraphCache(
                        restHandlerConfiguration.getTimeout(),
                        Duration.ofMillis(restHandlerConfiguration.getRefreshInterval())),
                Executors.directExecutor(),
                tracker,
                STATS_REFRESH_INTERVAL);
    }

    private static ArchivedExecutionJobVertex createJobVertex(
            ExecutionState state, int parallelism) {
        ArchivedExecutionVertex[] vertices = new ArchivedExecutionVertex[parallelism];
        for (int i = 0; i < parallelism; i++) {
            vertices[i] = generateExecutionVertex(i, state);
        }
        return new ArchivedExecutionJobVertex(
                vertices,
                JOB_VERTEX_ID,
                "test",
                parallelism,
                parallelism,
                new SlotSharingGroup(),
                ResourceProfile.UNKNOWN,
                new StringifiedAccumulatorResult[0]);
    }

    private static ArchivedExecutionVertex generateExecutionVertex(
            int subtaskIndex, ExecutionState executionState) {
        return new ArchivedExecutionVertex(
                subtaskIndex,
                "test task",
                new ArchivedExecution(
                        new StringifiedAccumulatorResult[0],
                        null,
                        createExecutionAttemptId(JOB_VERTEX_ID, subtaskIndex, 0),
                        executionState,
                        null,
                        null,
                        null,
                        new long[ExecutionState.values().length],
                        new long[ExecutionState.values().length]),
                new ExecutionHistory(0));
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(
            Integer subtaskIndex, Integer maxRecords) throws HandlerRequestException {
        final HashMap<String, String> pathParams = new HashMap<>(2);
        pathParams.put(JobIDPathParameter.KEY, JOB_ID.toString());
        pathParams.put(JobVertexIdPathParameter.KEY, JOB_VERTEX_ID.toString());

        Map<String, List<String>> queryParams = new HashMap<>();
        if (subtaskIndex != null) {
            queryParams.put(
                    SubtaskIndexQueryParameter.KEY,
                    Collections.singletonList(subtaskIndex.toString()));
        }
        if (maxRecords != null) {
            queryParams.put(
                    MaxRecordsQueryParameter.KEY, Collections.singletonList(maxRecords.toString()));
        }

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobVertexDataSampleParameters(),
                pathParams,
                queryParams,
                Collections.emptyList());
    }

    /** A tracker that returns pre-built stats (or empty if null). */
    private static class TestDataSampleTracker
            implements VertexStatsTracker<VertexDataSampleStats> {

        private final VertexDataSampleStats stats;

        public TestDataSampleTracker(VertexDataSampleStats stats) {
            this.stats = stats;
        }

        @Override
        public Optional<VertexDataSampleStats> getJobVertexStats(
                JobID jobId, AccessExecutionJobVertex vertex) {
            return Optional.ofNullable(stats);
        }

        @Override
        public Optional<VertexDataSampleStats> getExecutionVertexStats(
                JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex) {
            return Optional.ofNullable(stats);
        }

        @Override
        public void shutDown() throws FlinkException {}
    }
}
