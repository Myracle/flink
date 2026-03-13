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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorDataSampleGateway;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Coordinator for data sampling requests. Unlike {@link
 * org.apache.flink.runtime.webmonitor.stats.TaskStatsRequestCoordinator}, this coordinator supports
 * partial success: when some TMs fail, results from successful TMs are still returned.
 *
 * <p>All three resolution states (COMPLETE, PARTIAL, FAILED) complete the future normally (not
 * exceptionally), ensuring results are cacheable by the tracker.
 */
@Internal
public class DataSampleRequestCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DataSampleRequestCoordinator.class);

    private static final int MAX_CONCURRENT_ROUNDS = 5;
    private static final int NUM_GHOST_SAMPLE_IDS = 10;
    private static final int MAX_TOTAL_RECORDS = 5000;
    private static final int FLATTEN_LIMIT = MAX_TOTAL_RECORDS * 2;

    private final Object lock = new Object();
    private final Executor executor;
    private final Duration requestTimeout;

    @GuardedBy("lock")
    private final Map<Integer, PendingSamplingRound> pendingRequests = new HashMap<>();

    @GuardedBy("lock")
    private final ArrayDeque<Integer> recentRequestIds = new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

    @GuardedBy("lock")
    private int requestIdCounter;

    @GuardedBy("lock")
    private int activeRoundCount;

    @GuardedBy("lock")
    private boolean isShutDown;

    public DataSampleRequestCoordinator(Executor executor, Duration requestTimeout) {
        checkNotNull(requestTimeout, "The request timeout must not be null.");
        checkArgument(requestTimeout.toMillis() >= 0L, "The request timeout must be non-negative.");
        this.executor = checkNotNull(executor);
        this.requestTimeout = requestTimeout;
    }

    /**
     * Triggers a new data sampling round.
     *
     * @param executionsWithGateways mapping from TM-grouped execution attempt IDs to their gateway
     *     futures
     * @param samplingWindow duration of the sampling window
     * @param maxBufferCapacity maximum buffer capacity per output
     * @return future that completes with the assembled stats
     */
    public CompletableFuture<VertexDataSampleStats> triggerDataSampleRequest(
            Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
                    executionsWithGateways,
            Duration samplingWindow,
            int maxBufferCapacity) {

        synchronized (lock) {
            if (isShutDown) {
                return CompletableFuture.completedFuture(
                        createFailedStats(
                                -1,
                                SampleErrorCode.INTERNAL_ERROR,
                                "Coordinator is shut down",
                                Collections.emptyList()));
            }

            if (activeRoundCount >= MAX_CONCURRENT_ROUNDS) {
                return CompletableFuture.completedFuture(
                        createFailedStats(
                                -1,
                                SampleErrorCode.TOO_MANY_CONCURRENT_ROUNDS,
                                "Too many concurrent sampling rounds ("
                                        + MAX_CONCURRENT_ROUNDS
                                        + ")",
                                Collections.emptyList()));
            }

            final int roundId = requestIdCounter++;
            final Set<ImmutableSet<ExecutionAttemptID>> groups =
                    new HashSet<>(executionsWithGateways.keySet());

            // Empty executions → complete immediately with NO_DATA
            if (groups.isEmpty()) {
                return CompletableFuture.completedFuture(
                        new VertexDataSampleStats(
                                SampleStatus.NO_DATA,
                                System.currentTimeMillis(),
                                roundId,
                                Collections.emptyList(),
                                0,
                                false,
                                0,
                                0,
                                Collections.emptyList(),
                                null,
                                null));
            }

            final PendingSamplingRound round = new PendingSamplingRound(roundId, groups);

            pendingRequests.put(roundId, round);
            activeRoundCount++;

            Duration totalTimeout = requestTimeout.plusMillis(samplingWindow.toMillis());
            DataSampleRequest request =
                    new DataSampleRequest(roundId, samplingWindow, maxBufferCapacity);

            for (Map.Entry<
                            ImmutableSet<ExecutionAttemptID>,
                            CompletableFuture<TaskExecutorDataSampleGateway>>
                    entry : executionsWithGateways.entrySet()) {

                final ImmutableSet<ExecutionAttemptID> group = entry.getKey();
                final CompletableFuture<TaskExecutorDataSampleGateway> gatewayFuture =
                        entry.getValue();

                gatewayFuture
                        .thenCompose(
                                gateway ->
                                        gateway.requestDataSamples(
                                                group.asList(), request, totalTimeout))
                        .whenCompleteAsync(
                                (response, throwable) -> {
                                    if (throwable != null) {
                                        handleFailedResponse(roundId, group, throwable);
                                    } else {
                                        handleSuccessfulResponse(roundId, group, response);
                                    }
                                },
                                executor);
            }

            // Schedule timeout
            CompletableFuture<?> timeoutFuture =
                    CompletableFuture.runAsync(
                            () -> handleTimeout(roundId),
                            CompletableFuture.delayedExecutor(
                                    totalTimeout.toMillis(),
                                    java.util.concurrent.TimeUnit.MILLISECONDS,
                                    executor));
            round.timeoutFuture = timeoutFuture;

            return round.resultFuture;
        }
    }

    private void handleSuccessfulResponse(
            int roundId, ImmutableSet<ExecutionAttemptID> group, TaskDataSampleResponse response) {
        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            PendingSamplingRound round = pendingRequests.get(roundId);
            if (round != null) {
                if (round.remainingGroups.remove(group)) {
                    round.successResults.put(group, response);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Received successful data sample response for round {},"
                                        + " group size {}",
                                roundId,
                                group.size());
                    }
                    checkAndComplete(round);
                }
            } else if (recentRequestIds.contains(roundId)) {
                LOG.debug("Received late data sample response for round {}", roundId);
            } else {
                LOG.debug("Unknown round ID {} in successful response", roundId);
            }
        }
    }

    private void handleFailedResponse(
            int roundId, ImmutableSet<ExecutionAttemptID> group, Throwable cause) {
        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            PendingSamplingRound round = pendingRequests.get(roundId);
            if (round != null) {
                if (round.remainingGroups.remove(group)) {
                    String errorMsg =
                            cause.getMessage() != null
                                    ? cause.getMessage()
                                    : cause.getClass().getSimpleName();
                    for (ExecutionAttemptID attemptId : group) {
                        round.failedSubtaskInfos.add(
                                new FailedSubtaskInfo(
                                        attemptId.getSubtaskIndex(),
                                        SampleErrorCode.TM_UNREACHABLE,
                                        errorMsg));
                    }
                    LOG.warn(
                            "Data sample request for round {} failed for group"
                                    + " of {} tasks: {}",
                            roundId,
                            group.size(),
                            errorMsg);
                    checkAndComplete(round);
                }
            } else if (recentRequestIds.contains(roundId)) {
                LOG.debug("Received late failed response for round {}", roundId);
            } else {
                LOG.debug("Unknown round ID {} in failed response", roundId);
            }
        }
    }

    private void handleTimeout(int roundId) {
        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            PendingSamplingRound round = pendingRequests.get(roundId);
            if (round != null && !round.remainingGroups.isEmpty()) {
                LOG.info(
                        "Data sample round {} timed out with {} remaining groups",
                        roundId,
                        round.remainingGroups.size());

                for (ImmutableSet<ExecutionAttemptID> group :
                        new ArrayList<>(round.remainingGroups)) {
                    round.remainingGroups.remove(group);
                    for (ExecutionAttemptID attemptId : group) {
                        round.failedSubtaskInfos.add(
                                new FailedSubtaskInfo(
                                        attemptId.getSubtaskIndex(),
                                        SampleErrorCode.TIMEOUT,
                                        "Request timed out"));
                    }
                }
                checkAndComplete(round);
            }
        }
    }

    @GuardedBy("lock")
    private void checkAndComplete(PendingSamplingRound round) {
        if (!round.remainingGroups.isEmpty()) {
            return;
        }

        pendingRequests.remove(round.roundId);
        activeRoundCount--;
        rememberRecentRequestId(round.roundId);

        // Cancel the timeout callback since this round is already complete
        if (round.timeoutFuture != null) {
            round.timeoutFuture.cancel(false);
        }

        boolean hasSuccess = !round.successResults.isEmpty();
        boolean hasFailure = !round.failedSubtaskInfos.isEmpty();

        SampleStatus status;
        if (hasSuccess && hasFailure) {
            status = SampleStatus.PARTIAL;
        } else if (hasSuccess) {
            status = SampleStatus.COMPLETE;
        } else {
            status = SampleStatus.FAILED;
        }

        VertexDataSampleStats stats = assembleStats(round, status);
        round.resultFuture.complete(stats);
    }

    @GuardedBy("lock")
    private VertexDataSampleStats assembleStats(PendingSamplingRound round, SampleStatus status) {
        long endTime = System.currentTimeMillis();

        // Flatten all responses into per-subtask samples (bounded by FLATTEN_LIMIT)
        Map<Integer, List<SampledRecord>> recordsBySubtask = new HashMap<>();
        int totalDroppedByContention = 0;
        int totalDroppedByRateLimit = 0;
        int totalFlattenedSoFar = 0;

        for (TaskDataSampleResponse response : round.successResults.values()) {
            for (Map.Entry<ExecutionAttemptID, SamplingRoundResult> entry :
                    response.getResults().entrySet()) {
                int subtaskIndex = entry.getKey().getSubtaskIndex();
                SamplingRoundResult result = entry.getValue();

                // Always accumulate drop counters
                totalDroppedByContention += result.getDroppedByContention();
                totalDroppedByRateLimit += result.getDroppedByRateLimit();

                List<SampledRecord> incoming = result.getRecords();
                int spaceLeft = FLATTEN_LIMIT - totalFlattenedSoFar;
                if (spaceLeft <= 0) {
                    continue;
                }

                List<SampledRecord> subtaskRecords =
                        recordsBySubtask.computeIfAbsent(subtaskIndex, k -> new ArrayList<>());
                if (incoming.size() <= spaceLeft) {
                    subtaskRecords.addAll(incoming);
                    totalFlattenedSoFar += incoming.size();
                } else {
                    subtaskRecords.addAll(incoming.subList(0, spaceLeft));
                    totalFlattenedSoFar += spaceLeft;
                }
            }
        }

        // Apply MAX_TOTAL_RECORDS cap with proportional fair distribution
        // TODO: Implement byte accounting per FLIP Appendix E (maxResponseBytes cap)
        int totalRecordCount = 0;
        for (List<SampledRecord> records : recordsBySubtask.values()) {
            totalRecordCount += records.size();
        }

        boolean totalTruncated = totalRecordCount > MAX_TOTAL_RECORDS;

        if (totalTruncated) {
            applyFairTruncation(recordsBySubtask, MAX_TOTAL_RECORDS);
        }

        // Build SubtaskDataSample list
        List<SubtaskDataSample> samples = new ArrayList<>();
        for (Map.Entry<Integer, List<SampledRecord>> entry : recordsBySubtask.entrySet()) {
            int subtaskIndex = entry.getKey();
            List<SampledRecord> records = entry.getValue();
            boolean subtaskTruncated =
                    totalTruncated || records.stream().anyMatch(SampledRecord::isTruncated);
            samples.add(
                    new SubtaskDataSample(subtaskIndex, records, records.size(), subtaskTruncated));
        }

        // Determine error info for FAILED status
        SampleErrorCode errorCode = null;
        String errorMessage = null;
        if (status == SampleStatus.FAILED && !round.failedSubtaskInfos.isEmpty()) {
            errorCode = round.failedSubtaskInfos.get(0).getErrorCode();
            errorMessage = round.failedSubtaskInfos.get(0).getErrorMessage();
        }

        // Check if all subtasks returned NO_DATA
        if (status == SampleStatus.COMPLETE && samples.isEmpty()) {
            status = SampleStatus.NO_DATA;
        }

        return new VertexDataSampleStats(
                status,
                endTime,
                round.roundId,
                samples,
                totalRecordCount,
                totalTruncated,
                totalDroppedByContention,
                totalDroppedByRateLimit,
                round.failedSubtaskInfos,
                errorCode,
                errorMessage);
    }

    /**
     * Proportional fair truncation: distribute the budget across subtasks so that no subtask gets
     * more than its fair share. Subtasks with fewer records than their share keep all records; the
     * surplus is redistributed to remaining subtasks.
     */
    @VisibleForTesting
    static void applyFairTruncation(
            Map<Integer, List<SampledRecord>> recordsBySubtask, int maxTotal) {
        if (recordsBySubtask.isEmpty()) {
            return;
        }

        // Sort subtask entries by record count ascending for fair distribution
        List<Map.Entry<Integer, List<SampledRecord>>> entries =
                new ArrayList<>(recordsBySubtask.entrySet());
        entries.sort((a, b) -> Integer.compare(a.getValue().size(), b.getValue().size()));

        int remaining = maxTotal;
        int subtasksLeft = entries.size();

        for (Map.Entry<Integer, List<SampledRecord>> entry : entries) {
            int fairShare = remaining / subtasksLeft;
            List<SampledRecord> records = entry.getValue();
            if (records.size() > fairShare) {
                // Truncate to fair share
                recordsBySubtask.put(
                        entry.getKey(), new ArrayList<>(records.subList(0, fairShare)));
                remaining -= fairShare;
            } else {
                remaining -= records.size();
            }
            subtasksLeft--;
        }
    }

    private static VertexDataSampleStats createFailedStats(
            int roundId,
            SampleErrorCode errorCode,
            String errorMessage,
            List<FailedSubtaskInfo> failedSubtasks) {
        return new VertexDataSampleStats(
                SampleStatus.FAILED,
                System.currentTimeMillis(),
                roundId,
                Collections.emptyList(),
                0,
                false,
                0,
                0,
                failedSubtasks,
                errorCode,
                errorMessage);
    }

    @GuardedBy("lock")
    private void rememberRecentRequestId(int requestId) {
        if (recentRequestIds.size() >= NUM_GHOST_SAMPLE_IDS) {
            recentRequestIds.removeFirst();
        }
        recentRequestIds.addLast(requestId);
    }

    /** Shuts down the coordinator. After shut down, no further operations are executed. */
    public void shutDown() {
        synchronized (lock) {
            if (!isShutDown) {
                LOG.info("Shutting down data sample request coordinator.");

                for (PendingSamplingRound round : pendingRequests.values()) {
                    round.resultFuture.complete(
                            createFailedStats(
                                    round.roundId,
                                    SampleErrorCode.INTERNAL_ERROR,
                                    "Coordinator shut down",
                                    round.failedSubtaskInfos));
                }

                pendingRequests.clear();
                recentRequestIds.clear();
                activeRoundCount = 0;
                isShutDown = true;
            }
        }
    }

    @VisibleForTesting
    public int getNumberOfPendingRequests() {
        synchronized (lock) {
            return pendingRequests.size();
        }
    }

    // ------------------------------------------------------------------------

    /** A pending data sampling round that tracks responses from all TM groups. */
    private static class PendingSamplingRound {
        final int roundId;
        final long startTime;
        final Set<ImmutableSet<ExecutionAttemptID>> remainingGroups;
        final Map<ImmutableSet<ExecutionAttemptID>, TaskDataSampleResponse> successResults;
        final List<FailedSubtaskInfo> failedSubtaskInfos;
        final CompletableFuture<VertexDataSampleStats> resultFuture;

        /** Timeout future that can be cancelled when the round completes early. */
        CompletableFuture<?> timeoutFuture;

        PendingSamplingRound(int roundId, Set<ImmutableSet<ExecutionAttemptID>> groups) {
            this.roundId = roundId;
            this.startTime = System.currentTimeMillis();
            this.remainingGroups = new HashSet<>(groups);
            this.successResults = new HashMap<>();
            this.failedSubtaskInfos = new ArrayList<>();
            this.resultFuture = new CompletableFuture<>();
        }
    }
}
