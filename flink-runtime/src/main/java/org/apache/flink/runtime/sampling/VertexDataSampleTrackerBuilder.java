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
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Builder for {@link VertexDataSampleTracker}. */
public class VertexDataSampleTrackerBuilder {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final ScheduledExecutorService executor;
    private final Duration restTimeout;

    private DataSampleRequestCoordinator coordinator;
    private Duration cleanUpInterval;
    private Duration statsRefreshInterval;
    private Duration samplingWindow;
    private int maxBufferCapacity;
    private Cache<VertexDataSampleTracker.JobVertexKey, VertexDataSampleStats> statsCache;

    VertexDataSampleTrackerBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Duration restTimeout) {
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.executor = executor;
        this.restTimeout = restTimeout;
    }

    public VertexDataSampleTrackerBuilder setCoordinator(DataSampleRequestCoordinator coordinator) {
        this.coordinator = coordinator;
        return this;
    }

    public VertexDataSampleTrackerBuilder setCleanUpInterval(Duration cleanUpInterval) {
        this.cleanUpInterval = cleanUpInterval;
        return this;
    }

    public VertexDataSampleTrackerBuilder setStatsRefreshInterval(Duration statsRefreshInterval) {
        this.statsRefreshInterval = statsRefreshInterval;
        return this;
    }

    public VertexDataSampleTrackerBuilder setSamplingWindow(Duration samplingWindow) {
        this.samplingWindow = samplingWindow;
        return this;
    }

    public VertexDataSampleTrackerBuilder setMaxBufferCapacity(int maxBufferCapacity) {
        this.maxBufferCapacity = maxBufferCapacity;
        return this;
    }

    @VisibleForTesting
    VertexDataSampleTrackerBuilder setStatsCache(
            Cache<VertexDataSampleTracker.JobVertexKey, VertexDataSampleStats> statsCache) {
        this.statsCache = statsCache;
        return this;
    }

    public VertexDataSampleTracker build() {
        if (statsCache == null) {
            statsCache = defaultCache();
        }
        return new VertexDataSampleTracker(
                coordinator,
                resourceManagerGatewayRetriever,
                executor,
                cleanUpInterval,
                statsRefreshInterval,
                samplingWindow,
                maxBufferCapacity,
                restTimeout,
                statsCache);
    }

    private Cache<VertexDataSampleTracker.JobVertexKey, VertexDataSampleStats> defaultCache() {
        checkArgument(cleanUpInterval.toMillis() > 0, "Clean up interval must be greater than 0");
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    public static VertexDataSampleTrackerBuilder newBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Duration restTimeout) {
        return new VertexDataSampleTrackerBuilder(
                resourceManagerGatewayRetriever, executor, restTimeout);
    }
}
