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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataSampleResponseBody}. */
class DataSampleResponseBodyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testJsonRoundTrip() throws Exception {
        DataSampleResponseBody original =
                new DataSampleResponseBody(
                        "COMPLETE",
                        System.currentTimeMillis(),
                        42,
                        false,
                        10,
                        false,
                        1,
                        2,
                        "TIMEOUT",
                        "some error",
                        Collections.singletonList(
                                new DataSampleResponseBody.FailedSubtaskResponseBody(
                                        3, "TM_UNREACHABLE", "host down")),
                        Collections.singletonList(
                                new DataSampleResponseBody.SubtaskSampleResponseBody(
                                        0,
                                        Collections.singletonList(
                                                new DataSampleResponseBody
                                                        .SampledRecordResponseBody(
                                                        1000L,
                                                        2000L,
                                                        "{\"key\":\"value\"}",
                                                        "String",
                                                        false,
                                                        null)),
                                        5,
                                        false)));

        String json = MAPPER.writeValueAsString(original);
        DataSampleResponseBody deserialized = MAPPER.readValue(json, DataSampleResponseBody.class);

        assertThat(deserialized.getStatus()).isEqualTo(original.getStatus());
        assertThat(deserialized.getEndTimestamp()).isEqualTo(original.getEndTimestamp());
        assertThat(deserialized.getRoundId()).isEqualTo(original.getRoundId());
        assertThat(deserialized.isStale()).isEqualTo(original.isStale());
        assertThat(deserialized.getTotalRecordCount()).isEqualTo(original.getTotalRecordCount());
        assertThat(deserialized.isTotalTruncated()).isEqualTo(original.isTotalTruncated());
        assertThat(deserialized.getDroppedByContention())
                .isEqualTo(original.getDroppedByContention());
        assertThat(deserialized.getDroppedByRateLimit())
                .isEqualTo(original.getDroppedByRateLimit());
        assertThat(deserialized.getErrorCode()).isEqualTo(original.getErrorCode());
        assertThat(deserialized.getErrorMessage()).isEqualTo(original.getErrorMessage());
        assertThat(deserialized.getFailedSubtasks()).hasSize(1);
        assertThat(deserialized.getFailedSubtasks().get(0).getSubtaskIndex()).isEqualTo(3);
        assertThat(deserialized.getSamples()).hasSize(1);
        assertThat(deserialized.getSamples().get(0).getRecords()).hasSize(1);
        assertThat(deserialized.getSamples().get(0).getRecords().get(0).getData())
                .isEqualTo("{\"key\":\"value\"}");
    }

    @Test
    void testFromStatsConversion() {
        long now = System.currentTimeMillis();
        List<SampledRecord> records =
                Arrays.asList(
                        new SampledRecord(now, null, "rec0", "String", false, null),
                        new SampledRecord(now, 100L, "rec1", "Integer", true, "side"));

        SubtaskDataSample subtask0 = new SubtaskDataSample(0, records, 5, false);
        SubtaskDataSample subtask1 =
                new SubtaskDataSample(
                        1,
                        Collections.singletonList(
                                new SampledRecord(now, null, "rec2", "Long", false, null)),
                        2,
                        true);

        FailedSubtaskInfo failedInfo =
                new FailedSubtaskInfo(2, SampleErrorCode.TIMEOUT, "timed out");

        VertexDataSampleStats stats =
                new VertexDataSampleStats(
                        SampleStatus.PARTIAL,
                        now,
                        7,
                        Arrays.asList(subtask0, subtask1),
                        8,
                        true,
                        3,
                        4,
                        Collections.singletonList(failedInfo),
                        SampleErrorCode.TIMEOUT,
                        "partial failure");

        DataSampleResponseBody response =
                DataSampleResponseBody.fromStats(stats, false, null, null);

        assertThat(response.getStatus()).isEqualTo("PARTIAL");
        assertThat(response.getEndTimestamp()).isEqualTo(now);
        assertThat(response.getRoundId()).isEqualTo(7);
        assertThat(response.isStale()).isFalse();
        assertThat(response.getTotalRecordCount()).isEqualTo(8);
        assertThat(response.isTotalTruncated()).isTrue();
        assertThat(response.getDroppedByContention()).isEqualTo(3);
        assertThat(response.getDroppedByRateLimit()).isEqualTo(4);
        assertThat(response.getErrorCode()).isEqualTo("TIMEOUT");
        assertThat(response.getErrorMessage()).isEqualTo("partial failure");
        assertThat(response.getSamples()).hasSize(2);
        assertThat(response.getFailedSubtasks()).hasSize(1);
        assertThat(response.getFailedSubtasks().get(0).getSubtaskIndex()).isEqualTo(2);

        // Verify record details on subtask 0
        DataSampleResponseBody.SubtaskSampleResponseBody sub0 = response.getSamples().get(0);
        assertThat(sub0.getSubtaskIndex()).isEqualTo(0);
        assertThat(sub0.getRecords()).hasSize(2);
        assertThat(sub0.getRecords().get(1).getRecordTimestamp()).isEqualTo(100L);
        assertThat(sub0.getRecords().get(1).getSideOutputName()).isEqualTo("side");
    }

    @Test
    void testSubtaskFiltering() {
        long now = System.currentTimeMillis();
        SubtaskDataSample subtask0 =
                new SubtaskDataSample(
                        0,
                        Collections.singletonList(
                                new SampledRecord(now, null, "data0", "String", false, null)),
                        1,
                        false);
        SubtaskDataSample subtask1 =
                new SubtaskDataSample(
                        1,
                        Collections.singletonList(
                                new SampledRecord(now, null, "data1", "String", false, null)),
                        1,
                        false);

        VertexDataSampleStats stats =
                new VertexDataSampleStats(
                        SampleStatus.COMPLETE,
                        now,
                        1,
                        Arrays.asList(subtask0, subtask1),
                        2,
                        false,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null);

        DataSampleResponseBody response = DataSampleResponseBody.fromStats(stats, false, 1, null);

        assertThat(response.getSamples()).hasSize(1);
        assertThat(response.getSamples().get(0).getSubtaskIndex()).isEqualTo(1);
        assertThat(response.getSamples().get(0).getRecords().get(0).getData()).isEqualTo("data1");
    }

    @Test
    void testMaxRecordsFiltering() {
        long now = System.currentTimeMillis();
        List<SampledRecord> manyRecords =
                Arrays.asList(
                        new SampledRecord(now, null, "r0", "String", false, null),
                        new SampledRecord(now, null, "r1", "String", false, null),
                        new SampledRecord(now, null, "r2", "String", false, null),
                        new SampledRecord(now, null, "r3", "String", false, null),
                        new SampledRecord(now, null, "r4", "String", false, null));

        SubtaskDataSample subtask = new SubtaskDataSample(0, manyRecords, 5, false);

        VertexDataSampleStats stats =
                new VertexDataSampleStats(
                        SampleStatus.COMPLETE,
                        now,
                        1,
                        Collections.singletonList(subtask),
                        5,
                        false,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null);

        DataSampleResponseBody response = DataSampleResponseBody.fromStats(stats, false, null, 2);

        assertThat(response.getSamples()).hasSize(1);
        assertThat(response.getSamples().get(0).getRecords()).hasSize(2);
        assertThat(response.getSamples().get(0).getRecords().get(0).getData()).isEqualTo("r0");
        assertThat(response.getSamples().get(0).getRecords().get(1).getData()).isEqualTo("r1");
    }

    @Test
    void testDisabledResponse() {
        DataSampleResponseBody response = DataSampleResponseBody.disabled();
        assertThat(response.getStatus()).isEqualTo("DISABLED");
        assertThat(response.getSamples()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    void testWaitingResponse() {
        DataSampleResponseBody response = DataSampleResponseBody.waiting();
        assertThat(response.getStatus()).isEqualTo("PENDING");
        assertThat(response.getSamples()).isNull();
    }

    @Test
    void testTerminatedResponse() {
        DataSampleResponseBody response = DataSampleResponseBody.terminated();
        assertThat(response.getStatus()).isEqualTo("FAILED");
        assertThat(response.getErrorCode()).isEqualTo("TASK_TERMINATED");
        assertThat(response.getErrorMessage()).isEqualTo("Task has been terminated");
        assertThat(response.getEndTimestamp()).isNull();
        assertThat(response.getRoundId()).isNull();
    }

    @Test
    void testNullFieldsOmittedInJson() throws Exception {
        DataSampleResponseBody response = DataSampleResponseBody.waiting();
        String json = MAPPER.writeValueAsString(response);

        assertThat(json).doesNotContain("\"samples\"");
        assertThat(json).doesNotContain("\"failedSubtasks\"");
        assertThat(json).doesNotContain("\"errorCode\"");
        assertThat(json).doesNotContain("\"errorMessage\"");
        assertThat(json).doesNotContain("\"endTimestamp\"");
        assertThat(json).doesNotContain("\"roundId\"");
    }
}
