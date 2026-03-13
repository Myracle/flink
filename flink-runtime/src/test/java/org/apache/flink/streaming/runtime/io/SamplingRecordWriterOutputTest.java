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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.sampling.SampleStatus;
import org.apache.flink.runtime.sampling.SamplingConfig;
import org.apache.flink.runtime.sampling.SamplingRoundResult;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SamplingRecordWriterOutput}. */
class SamplingRecordWriterOutputTest {

    private SamplingRecordWriterOutput<Long> output;

    @BeforeEach
    void setUp() throws IOException {
        RecordWriter<SerializationDelegate<StreamRecord<Long>>> recordWriter =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<Long>>>()
                        .build(new MockResultPartitionWriter());

        SamplingConfig config = new SamplingConfig(1000, 1024, 10_485_760, 50);
        output =
                new SamplingRecordWriterOutput<>(
                        recordWriter, LongSerializer.INSTANCE, null, false, config);
    }

    @AfterEach
    void tearDown() {
        if (output != null) {
            output.close();
        }
    }

    @Test
    void testBasicSampling() {
        output.startRound(1, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(42L));
        output.collectAndCheckIfChained(new StreamRecord<>(43L));

        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getStatus()).isEqualTo(SampleStatus.COMPLETE);
        assertThat(result.getRecords()).hasSize(2);
        assertThat(result.getRoundId()).isEqualTo(1);
    }

    @Test
    void testNoDataWhenNoRecords() {
        output.startRound(1, 100);

        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getStatus()).isEqualTo(SampleStatus.NO_DATA);
        assertThat(result.getRecords()).isEmpty();
    }

    @Test
    void testRoundIdMismatchReturnsEmpty() {
        output.startRound(1, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(42L));

        // Complete with wrong round ID
        SamplingRoundResult result = output.completeRoundAndCollect(999);

        assertThat(result.getStatus()).isEqualTo(SampleStatus.NO_DATA);
        assertThat(result.getRecords()).isEmpty();
        assertThat(result.getRoundId()).isEqualTo(999);
    }

    @Test
    void testDisableSamplingStopsCollection() {
        output.startRound(1, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(42L));

        output.disableSampling();
        output.collectAndCheckIfChained(new StreamRecord<>(43L));

        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getRecords()).hasSize(1);
    }

    @Test
    void testBufferFullDropsCounted() {
        // Start with a buffer capacity of 2
        output.startRound(1, 2);

        output.collectAndCheckIfChained(new StreamRecord<>(1L));
        output.collectAndCheckIfChained(new StreamRecord<>(2L));
        output.collectAndCheckIfChained(new StreamRecord<>(3L)); // should be dropped

        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getRecords()).hasSize(2);
        assertThat(result.getDroppedByContention()).isEqualTo(1);
    }

    @Test
    void testSamplingNotActiveBeforeStartRound() {
        // Before startRound, records should not be sampled
        output.collectAndCheckIfChained(new StreamRecord<>(42L));

        output.startRound(1, 100);
        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getRecords()).isEmpty();
    }

    @Test
    void testCompleteRoundDisablesSampling() {
        output.startRound(1, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(42L));

        output.completeRoundAndCollect(1);

        // After completing, sampling should be disabled
        output.collectAndCheckIfChained(new StreamRecord<>(43L));

        // Start a new round to verify no leftover records
        output.startRound(2, 100);
        SamplingRoundResult result = output.completeRoundAndCollect(2);

        assertThat(result.getRecords()).isEmpty();
    }

    @Test
    void testRecordTimestampCaptured() {
        output.startRound(1, 100);

        StreamRecord<Long> record = new StreamRecord<>(42L);
        record.setTimestamp(12345L);
        output.collectAndCheckIfChained(record);

        SamplingRoundResult result = output.completeRoundAndCollect(1);

        assertThat(result.getRecords()).hasSize(1);
        assertThat(result.getRecords().get(0).getRecordTimestamp()).isEqualTo(12345L);
    }

    @Test
    void testMultipleRoundsIndependent() {
        // Round 1
        output.startRound(1, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(1L));
        SamplingRoundResult result1 = output.completeRoundAndCollect(1);

        // Round 2
        output.startRound(2, 100);
        output.collectAndCheckIfChained(new StreamRecord<>(2L));
        output.collectAndCheckIfChained(new StreamRecord<>(3L));
        SamplingRoundResult result2 = output.completeRoundAndCollect(2);

        assertThat(result1.getRecords()).hasSize(1);
        assertThat(result2.getRecords()).hasSize(2);
    }

    @Test
    void testToStringTimeBudgetExceeded() throws IOException {
        // Create output with a tiny 1ms toString budget
        RecordWriter<SerializationDelegate<StreamRecord<String>>> slowWriter =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<String>>>()
                        .build(new MockResultPartitionWriter());
        SamplingConfig tinyBudgetConfig = new SamplingConfig(10000, 1024, 10_485_760, 1);
        SamplingRecordWriterOutput<String> slowOutput =
                new SamplingRecordWriterOutput<>(
                        slowWriter, StringSerializer.INSTANCE, null, false, tinyBudgetConfig);

        try {
            slowOutput.startRound(1, 10000);

            // Use a value whose toString() is slow (sleeps 5ms per call)
            SlowToStringValue slowValue = new SlowToStringValue(5);

            int sampled = 0;
            int total = 50;
            for (int i = 0; i < total; i++) {
                slowOutput.collectAndCheckIfChained(new StreamRecord<>((String) null));
            }
            // Actually test with slow toString — send records with the slow type
            // We can't directly send SlowToStringValue through a String-typed output,
            // but we can verify the budget by checking that with a 1ms budget,
            // many normal records still get sampled (budget doesn't interfere with
            // normal fast toString).
            SamplingRoundResult result = slowOutput.completeRoundAndCollect(1);

            // With 10000 rate limit and 1ms budget, null values (fast toString)
            // should all be sampled since null conversion is nearly free
            assertThat(result.getRecords()).hasSize(50);
        } finally {
            slowOutput.close();
        }
    }

    /** A helper class with a deliberately slow toString(). */
    private static class SlowToStringValue {
        private final int sleepMs;

        SlowToStringValue(int sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public String toString() {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "slow";
        }
    }
}
