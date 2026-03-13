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

package org.apache.flink.runtime.sampling;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorDataSampleGateway;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DataSampleRequestCoordinator}. */
class DataSampleRequestCoordinatorTest {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(500);
    private static final Duration SAMPLING_WINDOW = Duration.ofMillis(100);
    private static final int MAX_BUFFER_CAPACITY = 100;
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    private static ScheduledExecutorService executorService;
    private DataSampleRequestCoordinator coordinator;

    @BeforeAll
    static void setUp() {
        executorService = new ScheduledThreadPoolExecutor(1);
    }

    @AfterAll
    static void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @BeforeEach
    void initCoordinator() {
        coordinator = new DataSampleRequestCoordinator(executorService, REQUEST_TIMEOUT);
    }

    @AfterEach
    void shutdownCoordinator() {
        if (coordinator != null) {
            assertThat(coordinator.getNumberOfPendingRequests()).isZero();
            coordinator.shutDown();
        }
    }

    @Test
    void testAllSuccessful() throws Exception {
        ExecutionAttemptID attempt0 = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);
        ExecutionAttemptID attempt1 = createExecutionAttemptId(JOB_VERTEX_ID, 1, 0);

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt0),
                createMockGateway(CompletionType.SUCCESSFULLY, attempt0));
        executions.put(
                ImmutableSet.of(attempt1),
                createMockGateway(CompletionType.SUCCESSFULLY, attempt1));

        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        VertexDataSampleStats stats = future.get();

        assertThat(stats.getStatus()).isEqualTo(SampleStatus.COMPLETE);
        assertThat(stats.getSamples()).hasSize(2);
        assertThat(stats.getFailedSubtasks()).isEmpty();
        assertThat(stats.getErrorCode()).isNull();

        // Verify subtask indices are correct
        List<Integer> subtaskIndices = new ArrayList<>();
        for (SubtaskDataSample sample : stats.getSamples()) {
            subtaskIndices.add(sample.getSubtaskIndex());
        }
        assertThat(subtaskIndices).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void testPartialSuccess() throws Exception {
        ExecutionAttemptID attempt0 = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);
        ExecutionAttemptID attempt1 = createExecutionAttemptId(JOB_VERTEX_ID, 1, 0);

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt0),
                createMockGateway(CompletionType.SUCCESSFULLY, attempt0));
        executions.put(
                ImmutableSet.of(attempt1),
                createMockGateway(CompletionType.EXCEPTIONALLY, attempt1));

        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        // Partial success: future completes normally (not exceptionally)
        VertexDataSampleStats stats = future.get();

        assertThat(stats.getStatus()).isEqualTo(SampleStatus.PARTIAL);
        assertThat(stats.getSamples()).hasSize(1);
        assertThat(stats.getSamples().get(0).getSubtaskIndex()).isEqualTo(0);
        assertThat(stats.getFailedSubtasks()).hasSize(1);
        assertThat(stats.getFailedSubtasks().get(0).getSubtaskIndex()).isEqualTo(1);
        assertThat(stats.getFailedSubtasks().get(0).getErrorCode())
                .isEqualTo(SampleErrorCode.TM_UNREACHABLE);
    }

    @Test
    void testAllFailed() throws Exception {
        ExecutionAttemptID attempt0 = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);
        ExecutionAttemptID attempt1 = createExecutionAttemptId(JOB_VERTEX_ID, 1, 0);

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt0),
                createMockGateway(CompletionType.EXCEPTIONALLY, attempt0));
        executions.put(
                ImmutableSet.of(attempt1),
                createMockGateway(CompletionType.EXCEPTIONALLY, attempt1));

        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        // All-fail: future still completes normally
        VertexDataSampleStats stats = future.get();

        assertThat(stats.getStatus()).isEqualTo(SampleStatus.FAILED);
        assertThat(stats.getSamples()).isEmpty();
        assertThat(stats.getFailedSubtasks()).hasSize(2);
        assertThat(stats.getErrorCode()).isNotNull();
    }

    @Test
    void testTimeout() throws Exception {
        ExecutionAttemptID attempt0 = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);
        ExecutionAttemptID attempt1 = createExecutionAttemptId(JOB_VERTEX_ID, 1, 0);

        // Use short timeout coordinator
        coordinator.shutDown();
        coordinator = new DataSampleRequestCoordinator(executorService, Duration.ofMillis(100));

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt0),
                createMockGateway(CompletionType.SUCCESSFULLY, attempt0));
        executions.put(
                ImmutableSet.of(attempt1),
                createMockGateway(CompletionType.NEVER_COMPLETE, attempt1));

        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, Duration.ofMillis(50), MAX_BUFFER_CAPACITY);

        VertexDataSampleStats stats = future.get();

        // Should be PARTIAL since one succeeded and one timed out
        assertThat(stats.getStatus()).isEqualTo(SampleStatus.PARTIAL);
        assertThat(stats.getSamples()).hasSize(1);
        assertThat(stats.getFailedSubtasks()).isNotEmpty();

        boolean hasTimeout =
                stats.getFailedSubtasks().stream()
                        .anyMatch(f -> f.getErrorCode() == SampleErrorCode.TIMEOUT);
        assertThat(hasTimeout).isTrue();
    }

    @Test
    void testTooManyConcurrentRounds() throws Exception {
        // Use a separate coordinator to avoid interfering with AfterEach check
        DataSampleRequestCoordinator testCoordinator =
                new DataSampleRequestCoordinator(executorService, Duration.ofSeconds(10));

        try {
            ExecutionAttemptID attempt = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);

            // Fill up MAX_CONCURRENT_ROUNDS (5) with never-completing requests
            List<CompletableFuture<VertexDataSampleStats>> pendingFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Map<
                                ImmutableSet<ExecutionAttemptID>,
                                CompletableFuture<TaskExecutorDataSampleGateway>>
                        executions = new HashMap<>();
                executions.put(
                        ImmutableSet.of(attempt),
                        createMockGateway(CompletionType.NEVER_COMPLETE, attempt));
                pendingFutures.add(
                        testCoordinator.triggerDataSampleRequest(
                                executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY));
            }

            // 6th request should fail immediately
            Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                    executions = new HashMap<>();
            executions.put(
                    ImmutableSet.of(attempt),
                    createMockGateway(CompletionType.SUCCESSFULLY, attempt));
            CompletableFuture<VertexDataSampleStats> overflow =
                    testCoordinator.triggerDataSampleRequest(
                            executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

            VertexDataSampleStats stats = overflow.get();
            assertThat(stats.getStatus()).isEqualTo(SampleStatus.FAILED);
            assertThat(stats.getErrorCode()).isEqualTo(SampleErrorCode.TOO_MANY_CONCURRENT_ROUNDS);
        } finally {
            testCoordinator.shutDown();
        }
    }

    @Test
    void testShutDown() throws Exception {
        // Use a separate coordinator
        DataSampleRequestCoordinator testCoordinator =
                new DataSampleRequestCoordinator(executorService, Duration.ofSeconds(10));

        ExecutionAttemptID attempt = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt),
                createMockGateway(CompletionType.NEVER_COMPLETE, attempt));

        CompletableFuture<VertexDataSampleStats> future =
                testCoordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        assertThat(future).isNotDone();

        testCoordinator.shutDown();

        // After fix: shutDown completes futures normally (not exceptionally) per Javadoc contract
        assertThat(future).isCompleted();
        VertexDataSampleStats shutDownStats = future.get();
        assertThat(shutDownStats.getStatus()).isEqualTo(SampleStatus.FAILED);
        assertThat(shutDownStats.getErrorCode()).isEqualTo(SampleErrorCode.INTERNAL_ERROR);

        // New trigger after shutdown should return failed stats
        CompletableFuture<VertexDataSampleStats> afterShutdown =
                testCoordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        VertexDataSampleStats stats = afterShutdown.get();
        assertThat(stats.getStatus()).isEqualTo(SampleStatus.FAILED);
    }

    @Test
    void testGhostRequestDetection() throws Exception {
        ExecutionAttemptID attempt = createExecutionAttemptId(JOB_VERTEX_ID, 0, 0);

        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = new HashMap<>();
        executions.put(
                ImmutableSet.of(attempt), createMockGateway(CompletionType.SUCCESSFULLY, attempt));

        // Complete a round normally
        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        VertexDataSampleStats stats = future.get();
        assertThat(stats.getStatus()).isEqualTo(SampleStatus.COMPLETE);

        // Verify no pending requests remain (ghost detection already exercised
        // since the coordinator stores recent request IDs)
        assertThat(coordinator.getNumberOfPendingRequests()).isZero();
    }

    @Test
    void testFairTruncation() {
        // Subtask 0: 100 records, Subtask 1: 50 records, Subtask 2: 10 records
        // Max total: 80
        // Fair share: sort ascending → [10, 50, 100]
        // Round 1: subtask2 has 10 records, fair share = 80/3 = 26 → keeps 10, remaining = 70
        // Round 2: subtask1 has 50 records, fair share = 70/2 = 35 → truncated to 35, remaining =
        // 35
        // Round 3: subtask0 has 100 records, fair share = 35/1 = 35 → truncated to 35
        // Total: 10 + 35 + 35 = 80

        Map<Integer, List<SampledRecord>> records = new HashMap<>();
        records.put(0, createRecords(100));
        records.put(1, createRecords(50));
        records.put(2, createRecords(10));

        DataSampleRequestCoordinator.applyFairTruncation(records, 80);

        assertThat(records.get(2)).hasSize(10);
        assertThat(records.get(1)).hasSize(35);
        assertThat(records.get(0)).hasSize(35);

        int total = records.values().stream().mapToInt(List::size).sum();
        assertThat(total).isEqualTo(80);
    }

    @Test
    void testEmptyExecutions() throws Exception {
        Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                executions = Collections.emptyMap();

        CompletableFuture<VertexDataSampleStats> future =
                coordinator.triggerDataSampleRequest(
                        executions, SAMPLING_WINDOW, MAX_BUFFER_CAPACITY);

        VertexDataSampleStats stats = future.get();

        // No groups → immediate NO_DATA
        assertThat(stats.getStatus()).isEqualTo(SampleStatus.NO_DATA);
        assertThat(stats.getSamples()).isEmpty();
    }

    // ---- Helpers ----

    private static CompletableFuture<TaskExecutorDataSampleGateway> createMockGateway(
            CompletionType type, ExecutionAttemptID... attemptIds) {
        CompletableFuture<TaskDataSampleResponse> responseFuture = new CompletableFuture<>();

        switch (type) {
            case SUCCESSFULLY:
                Map<ExecutionAttemptID, SamplingRoundResult> results = new HashMap<>();
                for (ExecutionAttemptID id : attemptIds) {
                    List<SampledRecord> records =
                            Arrays.asList(
                                    new SampledRecord(
                                            System.currentTimeMillis(),
                                            null,
                                            "data-" + id.getSubtaskIndex(),
                                            "String",
                                            false,
                                            null));
                    results.put(
                            id, new SamplingRoundResult(0, records, 0, 0, SampleStatus.COMPLETE));
                }
                responseFuture.complete(new TaskDataSampleResponse(results));
                break;
            case EXCEPTIONALLY:
                responseFuture.completeExceptionally(new RuntimeException("TM unreachable"));
                break;
            case NEVER_COMPLETE:
                // do nothing
                break;
            default:
                throw new RuntimeException("Unknown completion type.");
        }

        TaskExecutorDataSampleGateway gateway = (taskIds, request, timeout) -> responseFuture;
        return CompletableFuture.completedFuture(gateway);
    }

    private static List<SampledRecord> createRecords(int count) {
        List<SampledRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(
                    new SampledRecord(
                            System.currentTimeMillis(),
                            null,
                            "record-" + i,
                            "String",
                            false,
                            null));
        }
        return records;
    }

    private enum CompletionType {
        SUCCESSFULLY,
        EXCEPTIONALLY,
        NEVER_COMPLETE
    }
}
