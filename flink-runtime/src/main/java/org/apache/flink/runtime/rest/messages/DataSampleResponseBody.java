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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.sampling.FailedSubtaskInfo;
import org.apache.flink.runtime.sampling.SampleErrorCode;
import org.apache.flink.runtime.sampling.SampleStatus;
import org.apache.flink.runtime.sampling.SampledRecord;
import org.apache.flink.runtime.sampling.SubtaskDataSample;
import org.apache.flink.runtime.sampling.VertexDataSampleStats;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Response body for the data sample REST endpoint. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataSampleResponseBody implements ResponseBody {

    private static final String FIELD_STATUS = "status";
    private static final String FIELD_END_TIMESTAMP = "endTimestamp";
    private static final String FIELD_ROUND_ID = "roundId";
    private static final String FIELD_STALE = "stale";
    private static final String FIELD_TOTAL_RECORD_COUNT = "totalRecordCount";
    private static final String FIELD_TOTAL_TRUNCATED = "totalTruncated";
    private static final String FIELD_DROPPED_BY_CONTENTION = "droppedByContention";
    private static final String FIELD_DROPPED_BY_RATE_LIMIT = "droppedByRateLimit";
    private static final String FIELD_ERROR_CODE = "errorCode";
    private static final String FIELD_ERROR_MESSAGE = "errorMessage";
    private static final String FIELD_FAILED_SUBTASKS = "failedSubtasks";
    private static final String FIELD_SAMPLES = "samples";

    @JsonProperty(FIELD_STATUS)
    private final String status;

    @Nullable
    @JsonProperty(FIELD_END_TIMESTAMP)
    private final Long endTimestamp;

    @Nullable
    @JsonProperty(FIELD_ROUND_ID)
    private final Integer roundId;

    @JsonProperty(FIELD_STALE)
    private final boolean stale;

    @JsonProperty(FIELD_TOTAL_RECORD_COUNT)
    private final int totalRecordCount;

    @JsonProperty(FIELD_TOTAL_TRUNCATED)
    private final boolean totalTruncated;

    @JsonProperty(FIELD_DROPPED_BY_CONTENTION)
    private final int droppedByContention;

    @JsonProperty(FIELD_DROPPED_BY_RATE_LIMIT)
    private final int droppedByRateLimit;

    @Nullable
    @JsonProperty(FIELD_ERROR_CODE)
    private final String errorCode;

    @Nullable
    @JsonProperty(FIELD_ERROR_MESSAGE)
    private final String errorMessage;

    @Nullable
    @JsonProperty(FIELD_FAILED_SUBTASKS)
    private final List<FailedSubtaskResponseBody> failedSubtasks;

    @Nullable
    @JsonProperty(FIELD_SAMPLES)
    private final List<SubtaskSampleResponseBody> samples;

    @JsonCreator
    public DataSampleResponseBody(
            @JsonProperty(FIELD_STATUS) String status,
            @Nullable @JsonProperty(FIELD_END_TIMESTAMP) Long endTimestamp,
            @Nullable @JsonProperty(FIELD_ROUND_ID) Integer roundId,
            @JsonProperty(FIELD_STALE) boolean stale,
            @JsonProperty(FIELD_TOTAL_RECORD_COUNT) int totalRecordCount,
            @JsonProperty(FIELD_TOTAL_TRUNCATED) boolean totalTruncated,
            @JsonProperty(FIELD_DROPPED_BY_CONTENTION) int droppedByContention,
            @JsonProperty(FIELD_DROPPED_BY_RATE_LIMIT) int droppedByRateLimit,
            @Nullable @JsonProperty(FIELD_ERROR_CODE) String errorCode,
            @Nullable @JsonProperty(FIELD_ERROR_MESSAGE) String errorMessage,
            @Nullable @JsonProperty(FIELD_FAILED_SUBTASKS)
                    List<FailedSubtaskResponseBody> failedSubtasks,
            @Nullable @JsonProperty(FIELD_SAMPLES) List<SubtaskSampleResponseBody> samples) {
        this.status = status;
        this.endTimestamp = endTimestamp;
        this.roundId = roundId;
        this.stale = stale;
        this.totalRecordCount = totalRecordCount;
        this.totalTruncated = totalTruncated;
        this.droppedByContention = droppedByContention;
        this.droppedByRateLimit = droppedByRateLimit;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.failedSubtasks = failedSubtasks;
        this.samples = samples;
    }

    public String getStatus() {
        return status;
    }

    @Nullable
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Nullable
    public Integer getRoundId() {
        return roundId;
    }

    public boolean isStale() {
        return stale;
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

    @Nullable
    public String getErrorCode() {
        return errorCode;
    }

    @Nullable
    public String getErrorMessage() {
        return errorMessage;
    }

    @Nullable
    public List<FailedSubtaskResponseBody> getFailedSubtasks() {
        return failedSubtasks;
    }

    @Nullable
    public List<SubtaskSampleResponseBody> getSamples() {
        return samples;
    }

    /** Creates a response from domain stats with optional post-filtering. */
    public static DataSampleResponseBody fromStats(
            VertexDataSampleStats stats,
            boolean stale,
            @Nullable Integer subtaskFilter,
            @Nullable Integer maxRecords) {

        List<SubtaskSampleResponseBody> samples = new ArrayList<>();
        for (SubtaskDataSample subtask : stats.getSamples()) {
            if (subtaskFilter != null && subtask.getSubtaskIndex() != subtaskFilter) {
                continue;
            }

            List<SampledRecordResponseBody> records =
                    subtask.getRecords().stream()
                            .map(SampledRecordResponseBody::fromDomain)
                            .collect(Collectors.toList());

            if (maxRecords != null && records.size() > maxRecords) {
                records = records.subList(0, maxRecords);
            }

            samples.add(
                    new SubtaskSampleResponseBody(
                            subtask.getSubtaskIndex(),
                            records,
                            subtask.getSampleCount(),
                            subtask.isTruncated()));
        }

        List<FailedSubtaskResponseBody> failedSubtasks = null;
        if (!stats.getFailedSubtasks().isEmpty()) {
            failedSubtasks =
                    stats.getFailedSubtasks().stream()
                            .map(FailedSubtaskResponseBody::fromDomain)
                            .collect(Collectors.toList());
        }

        return new DataSampleResponseBody(
                stats.getStatus().name(),
                stats.getEndTimestamp() > 0 ? stats.getEndTimestamp() : null,
                stats.getRoundId(),
                stale,
                stats.getTotalRecordCount(),
                stats.isTotalTruncated(),
                stats.getDroppedByContention(),
                stats.getDroppedByRateLimit(),
                stats.getErrorCode() != null ? stats.getErrorCode().name() : null,
                stats.getErrorMessage(),
                failedSubtasks,
                samples.isEmpty() ? null : samples);
    }

    public static DataSampleResponseBody disabled() {
        return new DataSampleResponseBody(
                SampleStatus.DISABLED.name(),
                null,
                null,
                false,
                0,
                false,
                0,
                0,
                null,
                null,
                null,
                null);
    }

    public static DataSampleResponseBody waiting() {
        return new DataSampleResponseBody(
                SampleStatus.PENDING.name(),
                null,
                null,
                false,
                0,
                false,
                0,
                0,
                null,
                null,
                null,
                null);
    }

    public static DataSampleResponseBody terminated() {
        return new DataSampleResponseBody(
                SampleStatus.FAILED.name(),
                null,
                null,
                false,
                0,
                false,
                0,
                0,
                SampleErrorCode.TASK_TERMINATED.name(),
                "Task has been terminated",
                null,
                null);
    }

    // Inner response body classes

    /** Response body for a single subtask's samples. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SubtaskSampleResponseBody {

        private static final String FIELD_SUBTASK_INDEX = "subtaskIndex";
        private static final String FIELD_RECORDS = "records";
        private static final String FIELD_SAMPLE_COUNT = "sampleCount";
        private static final String FIELD_TRUNCATED = "truncated";

        @JsonProperty(FIELD_SUBTASK_INDEX)
        private final int subtaskIndex;

        @JsonProperty(FIELD_RECORDS)
        private final List<SampledRecordResponseBody> records;

        @JsonProperty(FIELD_SAMPLE_COUNT)
        private final int sampleCount;

        @JsonProperty(FIELD_TRUNCATED)
        private final boolean truncated;

        @JsonCreator
        public SubtaskSampleResponseBody(
                @JsonProperty(FIELD_SUBTASK_INDEX) int subtaskIndex,
                @JsonProperty(FIELD_RECORDS) List<SampledRecordResponseBody> records,
                @JsonProperty(FIELD_SAMPLE_COUNT) int sampleCount,
                @JsonProperty(FIELD_TRUNCATED) boolean truncated) {
            this.subtaskIndex = subtaskIndex;
            this.records = records;
            this.sampleCount = sampleCount;
            this.truncated = truncated;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public List<SampledRecordResponseBody> getRecords() {
            return records;
        }

        public int getSampleCount() {
            return sampleCount;
        }

        public boolean isTruncated() {
            return truncated;
        }
    }

    /** Response body for a single sampled record. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SampledRecordResponseBody {

        private static final String FIELD_SAMPLE_TIMESTAMP = "sampleTimestamp";
        private static final String FIELD_RECORD_TIMESTAMP = "recordTimestamp";
        private static final String FIELD_DATA = "data";
        private static final String FIELD_DATA_TYPE = "dataType";
        private static final String FIELD_TRUNCATED = "truncated";
        private static final String FIELD_SIDE_OUTPUT_NAME = "sideOutputName";

        @JsonProperty(FIELD_SAMPLE_TIMESTAMP)
        private final long sampleTimestamp;

        @Nullable
        @JsonProperty(FIELD_RECORD_TIMESTAMP)
        private final Long recordTimestamp;

        @JsonProperty(FIELD_DATA)
        private final String data;

        @JsonProperty(FIELD_DATA_TYPE)
        private final String dataType;

        @JsonProperty(FIELD_TRUNCATED)
        private final boolean truncated;

        @Nullable
        @JsonProperty(FIELD_SIDE_OUTPUT_NAME)
        private final String sideOutputName;

        @JsonCreator
        public SampledRecordResponseBody(
                @JsonProperty(FIELD_SAMPLE_TIMESTAMP) long sampleTimestamp,
                @Nullable @JsonProperty(FIELD_RECORD_TIMESTAMP) Long recordTimestamp,
                @JsonProperty(FIELD_DATA) String data,
                @JsonProperty(FIELD_DATA_TYPE) String dataType,
                @JsonProperty(FIELD_TRUNCATED) boolean truncated,
                @Nullable @JsonProperty(FIELD_SIDE_OUTPUT_NAME) String sideOutputName) {
            this.sampleTimestamp = sampleTimestamp;
            this.recordTimestamp = recordTimestamp;
            this.data = data;
            this.dataType = dataType;
            this.truncated = truncated;
            this.sideOutputName = sideOutputName;
        }

        public long getSampleTimestamp() {
            return sampleTimestamp;
        }

        @Nullable
        public Long getRecordTimestamp() {
            return recordTimestamp;
        }

        public String getData() {
            return data;
        }

        public String getDataType() {
            return dataType;
        }

        public boolean isTruncated() {
            return truncated;
        }

        @Nullable
        public String getSideOutputName() {
            return sideOutputName;
        }

        static SampledRecordResponseBody fromDomain(SampledRecord record) {
            return new SampledRecordResponseBody(
                    record.getSampleTimestamp(),
                    record.getRecordTimestamp(),
                    record.getData(),
                    record.getDataType(),
                    record.isTruncated(),
                    record.getSideOutputName());
        }
    }

    /** Response body for a failed subtask. */
    public static class FailedSubtaskResponseBody {

        private static final String FIELD_SUBTASK_INDEX = "subtaskIndex";
        private static final String FIELD_ERROR_CODE = "errorCode";
        private static final String FIELD_ERROR_MESSAGE = "errorMessage";

        @JsonProperty(FIELD_SUBTASK_INDEX)
        private final int subtaskIndex;

        @JsonProperty(FIELD_ERROR_CODE)
        private final String errorCode;

        @JsonProperty(FIELD_ERROR_MESSAGE)
        private final String errorMessage;

        @JsonCreator
        public FailedSubtaskResponseBody(
                @JsonProperty(FIELD_SUBTASK_INDEX) int subtaskIndex,
                @JsonProperty(FIELD_ERROR_CODE) String errorCode,
                @JsonProperty(FIELD_ERROR_MESSAGE) String errorMessage) {
            this.subtaskIndex = subtaskIndex;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        static FailedSubtaskResponseBody fromDomain(FailedSubtaskInfo info) {
            return new FailedSubtaskResponseBody(
                    info.getSubtaskIndex(), info.getErrorCode().name(), info.getErrorMessage());
        }
    }
}
