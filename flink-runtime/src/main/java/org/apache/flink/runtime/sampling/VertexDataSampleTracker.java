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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorDataSampleGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Tracker of data sample stats for job vertices. */
public class VertexDataSampleTracker implements VertexStatsTracker<VertexDataSampleStats> {

    private static final Logger LOG = LoggerFactory.getLogger(VertexDataSampleTracker.class);

    private final Object lock = new Object();

    @GuardedBy("lock")
    private final DataSampleRequestCoordinator coordinator;

    private final ExecutorService executor;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    @GuardedBy("lock")
    private final Cache<JobVertexKey, VertexDataSampleStats> statsCache;

    @GuardedBy("lock")
    private final Set<JobVertexKey> pendingStats = new HashSet<>();

    private final Duration statsRefreshInterval;

    private final Duration samplingWindow;

    private final int maxBufferCapacity;

    private final Duration rpcTimeout;

    // Used for testing only: signals when the first sampling result is available.
    // Note: CompletableFuture can only be completed once, so this only fires for the first round.
    private final CompletableFuture<Void> resultAvailableFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean shutDown;

    VertexDataSampleTracker(
            DataSampleRequestCoordinator coordinator,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Duration cleanUpInterval,
            Duration statsRefreshInterval,
            Duration samplingWindow,
            int maxBufferCapacity,
            Duration rpcTimeout,
            Cache<JobVertexKey, VertexDataSampleStats> statsCache) {

        this.coordinator = checkNotNull(coordinator, "Data sample request coordinator");
        this.resourceManagerGatewayRetriever =
                checkNotNull(resourceManagerGatewayRetriever, "Gateway retriever");
        this.executor = checkNotNull(executor, "Scheduled executor");
        this.statsRefreshInterval =
                checkNotNull(statsRefreshInterval, "Statistics refresh interval");
        this.samplingWindow = checkNotNull(samplingWindow, "Sampling window");
        this.maxBufferCapacity = maxBufferCapacity;
        this.rpcTimeout = rpcTimeout;
        this.statsCache = checkNotNull(statsCache, "Stats cache");

        checkArgument(cleanUpInterval.toMillis() > 0, "Clean up interval must be greater than 0");
        checkArgument(
                statsRefreshInterval.toMillis() > 0,
                "Stats refresh interval must be greater than 0");

        executor.scheduleWithFixedDelay(
                this::cleanUpStatsCache,
                cleanUpInterval.toMillis(),
                cleanUpInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the latest cached stats for the given vertex, triggering a new sampling round if the
     * cache is empty or stale.
     *
     * <p>Lazy trigger semantics: on the first request, the cache is empty so this method triggers
     * sampling and returns {@code Optional.empty()}. The handler translates this to a PENDING
     * response. Subsequent requests within the refresh interval return the cached result.
     */
    @Override
    public Optional<VertexDataSampleStats> getJobVertexStats(
            JobID jobId, AccessExecutionJobVertex vertex) {
        synchronized (lock) {
            final JobVertexKey key = getKey(jobId, vertex);

            final VertexDataSampleStats stats = statsCache.getIfPresent(key);
            if (stats == null
                    || System.currentTimeMillis()
                            >= stats.getEndTime() + statsRefreshInterval.toMillis()) {
                triggerSamplingInternal(key, vertex);
            }
            return Optional.ofNullable(stats);
        }
    }

    @Override
    public Optional<VertexDataSampleStats> getExecutionVertexStats(
            JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex) {
        // Delegate to job vertex stats — subtask filtering is post-hoc in handler
        return getJobVertexStats(jobId, vertex);
    }

    public Duration getStatsRefreshInterval() {
        return statsRefreshInterval;
    }

    private void triggerSamplingInternal(
            final JobVertexKey key, final AccessExecutionJobVertex vertex) {
        assert (Thread.holdsLock(lock));

        if (shutDown) {
            return;
        }

        if (pendingStats.contains(key)) {
            return;
        }

        pendingStats.add(key);

        final CompletableFuture<ResourceManagerGateway> gatewayFuture =
                resourceManagerGatewayRetriever.getFuture();

        gatewayFuture
                .thenCompose(
                        (ResourceManagerGateway resourceManagerGateway) ->
                                coordinator.triggerDataSampleRequest(
                                        matchExecutionsWithGateways(
                                                vertex.getTaskVertices(), resourceManagerGateway),
                                        samplingWindow,
                                        maxBufferCapacity))
                .whenCompleteAsync(
                        (stats, throwable) -> {
                            synchronized (lock) {
                                try {
                                    if (shutDown) {
                                        return;
                                    }
                                    if (stats == null) {
                                        LOG.error(
                                                "Failed to gather data samples" + " for {}",
                                                vertex.getName(),
                                                throwable);
                                        return;
                                    }
                                    statsCache.put(key, stats);
                                    resultAvailableFuture.complete(null);
                                } catch (Throwable t) {
                                    LOG.error("Error during stats completion.", t);
                                } finally {
                                    pendingStats.remove(key);
                                }
                            }
                        },
                        executor);
    }

    private Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
            matchExecutionsWithGateways(
                    AccessExecutionVertex[] executionVertices,
                    ResourceManagerGateway resourceManagerGateway) {

        final Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> executionsByLocation =
                groupExecutionsByLocation(executionVertices);

        return mapExecutionsToGateways(resourceManagerGateway, executionsByLocation);
    }

    private Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorDataSampleGateway>>
            mapExecutionsToGateways(
                    ResourceManagerGateway resourceManagerGateway,
                    Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> verticesByLocation) {

        final Map<
                        ImmutableSet<ExecutionAttemptID>,
                        CompletableFuture<TaskExecutorDataSampleGateway>>
                executionsWithGateways = new HashMap<>();

        for (Map.Entry<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> entry :
                verticesByLocation.entrySet()) {
            TaskManagerLocation tmLocation = entry.getKey();
            ImmutableSet<ExecutionAttemptID> attemptIds = entry.getValue();

            CompletableFuture<TaskExecutorDataSampleGateway> taskExecutorGatewayFuture =
                    resourceManagerGateway.requestTaskExecutorDataSampleGateway(
                            tmLocation.getResourceID(), rpcTimeout);

            executionsWithGateways.put(attemptIds, taskExecutorGatewayFuture);
        }
        return executionsWithGateways;
    }

    private Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> groupExecutionsByLocation(
            AccessExecutionVertex[] executionVertices) {

        final Map<TaskManagerLocation, Set<ExecutionAttemptID>> executionAttemptsByLocation =
                new HashMap<>();

        for (AccessExecutionVertex executionVertex : executionVertices) {
            if (executionVertex.getExecutionState() != ExecutionState.RUNNING
                    && executionVertex.getExecutionState() != ExecutionState.INITIALIZING) {
                LOG.trace(
                        "{} not running or initializing, but {};" + " not sampling",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        executionVertex.getExecutionState());
                continue;
            }
            for (AccessExecution execution : executionVertex.getCurrentExecutions()) {
                TaskManagerLocation tmLocation = execution.getAssignedResourceLocation();
                if (tmLocation == null) {
                    LOG.trace("ExecutionVertex {} is currently not assigned", executionVertex);
                    continue;
                }
                Set<ExecutionAttemptID> groupedAttemptIds =
                        executionAttemptsByLocation.computeIfAbsent(
                                tmLocation, k -> new HashSet<>());

                ExecutionAttemptID attemptId = execution.getAttemptId();
                groupedAttemptIds.add(attemptId);
            }
        }

        return executionAttemptsByLocation.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, e -> ImmutableSet.copyOf(e.getValue())));
    }

    @VisibleForTesting
    void cleanUpStatsCache() {
        statsCache.cleanUp();
    }

    @Override
    public void shutDown() {
        synchronized (lock) {
            if (!shutDown) {
                statsCache.invalidateAll();
                pendingStats.clear();
                shutDown = true;
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> getResultAvailableFuture() {
        return resultAvailableFuture;
    }

    private static JobVertexKey getKey(JobID jobId, AccessExecutionJobVertex vertex) {
        return new JobVertexKey(jobId, vertex.getJobVertexId());
    }

    /** Cache key combining JobID and JobVertexID. */
    static class JobVertexKey {
        private final JobID jobId;
        private final JobVertexID jobVertexId;

        JobVertexKey(JobID jobId, JobVertexID jobVertexId) {
            this.jobId = jobId;
            this.jobVertexId = jobVertexId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobVertexKey that = (JobVertexKey) o;
            return Objects.equals(jobId, that.jobId)
                    && Objects.equals(jobVertexId, that.jobVertexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, jobVertexId);
        }
    }
}
