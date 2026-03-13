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

import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.DataSampleResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDataSampleHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDataSampleParameters;
import org.apache.flink.runtime.rest.messages.MaxRecordsQueryParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexQueryParameter;
import org.apache.flink.runtime.sampling.VertexDataSampleStats;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Request handler for the job vertex data sample endpoint. */
public class JobVertexDataSampleHandler
        extends AbstractJobVertexHandler<DataSampleResponseBody, JobVertexDataSampleParameters> {

    private final VertexStatsTracker<VertexDataSampleStats> tracker;
    private final Duration statsRefreshInterval;

    public JobVertexDataSampleHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            VertexStatsTracker<VertexDataSampleStats> tracker,
            Duration statsRefreshInterval) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                JobVertexDataSampleHeaders.getInstance(),
                executionGraphCache,
                executor);
        this.tracker = tracker;
        this.statsRefreshInterval = statsRefreshInterval;
    }

    @Override
    protected DataSampleResponseBody handleRequest(
            HandlerRequest<EmptyRequestBody> request, AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {

        @Nullable Integer subtaskIndex = getSubtaskIndex(request, jobVertex);
        @Nullable Integer maxRecords = getMaxRecords(request);

        if (isTerminated(jobVertex, subtaskIndex)) {
            return DataSampleResponseBody.terminated();
        }

        final Optional<VertexDataSampleStats> stats =
                tracker.getJobVertexStats(
                        request.getPathParameter(JobIDPathParameter.class), jobVertex);

        if (!stats.isPresent()) {
            return DataSampleResponseBody.waiting();
        }

        VertexDataSampleStats sampleStats = stats.get();
        boolean stale =
                System.currentTimeMillis()
                        >= sampleStats.getEndTime() + statsRefreshInterval.toMillis();

        return DataSampleResponseBody.fromStats(sampleStats, stale, subtaskIndex, maxRecords);
    }

    private boolean isTerminated(
            AccessExecutionJobVertex jobVertex, @Nullable Integer subtaskIndex) {
        if (subtaskIndex == null) {
            return jobVertex.getAggregateState().isTerminal();
        }
        AccessExecutionVertex executionVertex = jobVertex.getTaskVertices()[subtaskIndex];
        return executionVertex.getExecutionState().isTerminal();
    }

    @Nullable
    private static Integer getSubtaskIndex(
            HandlerRequest<?> request, AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {
        final List<Integer> subtaskIndexParameter =
                request.getQueryParameter(SubtaskIndexQueryParameter.class);

        if (subtaskIndexParameter.isEmpty()) {
            return null;
        }
        int subtaskIndex = subtaskIndexParameter.get(0);
        if (subtaskIndex >= jobVertex.getTaskVertices().length || subtaskIndex < 0) {
            throw new RestHandlerException(
                    "Invalid subtask index for vertex " + jobVertex.getJobVertexId(),
                    HttpResponseStatus.NOT_FOUND);
        }
        return subtaskIndex;
    }

    @Nullable
    private static Integer getMaxRecords(HandlerRequest<?> request) {
        final List<Integer> maxRecordsParameter =
                request.getQueryParameter(MaxRecordsQueryParameter.class);

        if (maxRecordsParameter.isEmpty()) {
            return null;
        }
        return maxRecordsParameter.get(0);
    }

    @Override
    public void close() throws Exception {
        tracker.shutDown();
    }

    public static AbstractRestHandler<?, ?, ?, ?> disabledHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders) {
        return new DisabledJobVertexDataSampleHandler(leaderRetriever, timeout, responseHeaders);
    }

    private static class DisabledJobVertexDataSampleHandler
            extends AbstractRestHandler<
                    RestfulGateway,
                    EmptyRequestBody,
                    DataSampleResponseBody,
                    JobVertexDataSampleParameters> {
        protected DisabledJobVertexDataSampleHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Duration timeout,
                Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    JobVertexDataSampleHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<DataSampleResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            return CompletableFuture.completedFuture(DataSampleResponseBody.disabled());
        }
    }
}
