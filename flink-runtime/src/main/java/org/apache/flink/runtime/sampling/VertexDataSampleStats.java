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
import org.apache.flink.runtime.webmonitor.stats.Statistics;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** JM-level assembled data sampling statistics for a job vertex, cached by the tracker. */
@Internal
public class VertexDataSampleStats implements Statistics {

    private final SampleStatus status;
    private final long endTimestamp;
    private final int roundId;
    private final List<SubtaskDataSample> samples;
    private final int totalRecordCount;
    private final boolean totalTruncated;
    private final int droppedByContention;
    private final int droppedByRateLimit;
    private final List<FailedSubtaskInfo> failedSubtasks;
    @Nullable private final SampleErrorCode errorCode;
    @Nullable private final String errorMessage;

    public VertexDataSampleStats(
            SampleStatus status,
            long endTimestamp,
            int roundId,
            List<SubtaskDataSample> samples,
            int totalRecordCount,
            boolean totalTruncated,
            int droppedByContention,
            int droppedByRateLimit,
            List<FailedSubtaskInfo> failedSubtasks,
            @Nullable SampleErrorCode errorCode,
            @Nullable String errorMessage) {
        this.status = status;
        this.endTimestamp = endTimestamp;
        this.roundId = roundId;
        this.samples = samples;
        this.totalRecordCount = totalRecordCount;
        this.totalTruncated = totalTruncated;
        this.droppedByContention = droppedByContention;
        this.droppedByRateLimit = droppedByRateLimit;
        this.failedSubtasks = failedSubtasks;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public static VertexDataSampleStats disabled() {
        return new VertexDataSampleStats(
                SampleStatus.DISABLED,
                -1,
                -1,
                Collections.emptyList(),
                0,
                false,
                0,
                0,
                Collections.emptyList(),
                null,
                null);
    }

    public static VertexDataSampleStats waiting() {
        return new VertexDataSampleStats(
                SampleStatus.PENDING,
                -1,
                -1,
                Collections.emptyList(),
                0,
                false,
                0,
                0,
                Collections.emptyList(),
                null,
                null);
    }

    @Override
    public long getEndTime() {
        return endTimestamp;
    }

    public SampleStatus getStatus() {
        return status;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public int getRoundId() {
        return roundId;
    }

    public List<SubtaskDataSample> getSamples() {
        return samples;
    }

    public int getTotalRecordCount() {
        return totalRecordCount;
    }

    public boolean isTotalTruncated() {
        return totalTruncated;
    }

    public int getDroppedByContention() {
        return droppedByContention;
    }

    public int getDroppedByRateLimit() {
        return droppedByRateLimit;
    }

    public List<FailedSubtaskInfo> getFailedSubtasks() {
        return failedSubtasks;
    }

    @Nullable
    public SampleErrorCode getErrorCode() {
        return errorCode;
    }

    @Nullable
    public String getErrorMessage() {
        return errorMessage;
    }
}
