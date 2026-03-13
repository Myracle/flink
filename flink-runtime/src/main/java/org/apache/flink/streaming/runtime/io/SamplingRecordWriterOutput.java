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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.sampling.BoundedSampleBuffer;
import org.apache.flink.runtime.sampling.SampleStatus;
import org.apache.flink.runtime.sampling.SampledRecord;
import org.apache.flink.runtime.sampling.SamplingConfig;
import org.apache.flink.runtime.sampling.SamplingRoundResult;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link RecordWriterOutput} that intercepts records on the hot path for data sampling.
 * Forwarding always happens first; sampling is best-effort and never blocks data processing.
 *
 * <p>Thread model: {@code collect()} and {@code startRound()}/{@code completeRoundAndCollect()} are
 * all called on the mailbox thread. The only cross-thread interaction is {@code disableSampling()}
 * which may be called from the timer thread to immediately stop sampling. The {@code
 * samplingEnabled} flag is volatile to ensure visibility across threads.
 */
@Internal
public class SamplingRecordWriterOutput<OUT> extends RecordWriterOutput<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(SamplingRecordWriterOutput.class);

    private static final ClassValue<Boolean> HAS_CUSTOM_TOSTRING =
            new ClassValue<Boolean>() {
                @Override
                protected Boolean computeValue(Class<?> clazz) {
                    try {
                        return clazz.getMethod("toString").getDeclaringClass() != Object.class;
                    } catch (Exception e) {
                        // Catches SecurityException from restricted environments
                        // (e.g., code-generated classes with security managers)
                        return false;
                    }
                }
            };

    private volatile boolean samplingEnabled;
    private final BoundedSampleBuffer<SampledRecord> sampleBuffer;
    private final SamplingConfig samplingConfig;

    // Rate limiter fields — accessed only from mailbox thread
    private int samplesThisSecond;
    private long currentSecondStart;

    // toString time budget fields — accessed only from mailbox thread
    private final long toStringBudgetNanos;
    private long toStringTimeUsedThisSecond;
    private long toStringBudgetWindowStart;

    // Drop counters — accessed only from mailbox thread.
    // Note: droppedByContention counts buffer-full drops (tryLock always succeeds on
    // mailbox thread). The name is kept for consistency with the REST API JSON field.
    private int droppedByContention;
    private int droppedByRateLimit;

    private int currentRoundId;

    @SuppressWarnings("unchecked")
    public SamplingRecordWriterOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            TypeSerializer<OUT> outSerializer,
            OutputTag outputTag,
            boolean supportsUnalignedCheckpoints,
            SamplingConfig samplingConfig) {
        super(recordWriter, outSerializer, outputTag, supportsUnalignedCheckpoints);
        this.samplingConfig = samplingConfig;
        this.sampleBuffer = new BoundedSampleBuffer<>(0);
        this.samplesThisSecond = 0;
        this.currentSecondStart = System.currentTimeMillis();
        this.toStringBudgetNanos = samplingConfig.getToStringBudgetNanos();
        this.toStringTimeUsedThisSecond = 0;
        this.toStringBudgetWindowStart = System.nanoTime();
    }

    @Override
    public boolean collectAndCheckIfChained(StreamRecord<OUT> record) {
        boolean result = super.collectAndCheckIfChained(record);
        if (samplingEnabled) {
            sampleRecord(record, null);
        }
        return result;
    }

    @Override
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
        boolean result = super.collectAndCheckIfChained(outputTag, record);
        if (samplingEnabled) {
            sampleSideOutputRecord(outputTag, record);
        }
        return result;
    }

    /** Starts a new sampling round. Must be called from the mailbox thread. */
    public void startRound(int roundId, int maxBufferCapacity) {
        this.currentRoundId = roundId;
        this.droppedByContention = 0;
        this.droppedByRateLimit = 0;
        this.samplesThisSecond = 0;
        this.currentSecondStart = System.currentTimeMillis();
        this.toStringTimeUsedThisSecond = 0;
        this.toStringBudgetWindowStart = System.nanoTime();
        this.sampleBuffer.reset(maxBufferCapacity);
        this.samplingEnabled = true;
    }

    /**
     * Disables sampling immediately. May be called from the timer thread to ensure the sampling
     * window is not exceeded even if the mailbox is backlogged.
     */
    public void disableSampling() {
        this.samplingEnabled = false;
    }

    /**
     * Completes the current sampling round and returns the result. Must be called from the mailbox
     * thread. If the passed {@code roundId} does not match the current round (e.g., a delayed
     * callback from a previous round), returns an empty NO_DATA result.
     */
    public SamplingRoundResult completeRoundAndCollect(int roundId) {
        this.samplingEnabled = false;
        if (roundId != this.currentRoundId) {
            return SamplingRoundResult.empty(roundId);
        }
        List<SampledRecord> records = sampleBuffer.drainAndClear();
        SampleStatus status = records.isEmpty() ? SampleStatus.NO_DATA : SampleStatus.COMPLETE;
        return new SamplingRoundResult(
                roundId, records, droppedByContention, droppedByRateLimit, status);
    }

    private void sampleRecord(StreamRecord<?> record, String sideOutputName) {
        long now = System.currentTimeMillis();
        if (!checkRateLimit(now)) {
            droppedByRateLimit++;
            return;
        }

        // toString time budget check (nanoTime is monotonic, independent of wall clock)
        long nanoNow = System.nanoTime();
        if (nanoNow - toStringBudgetWindowStart >= 1_000_000_000L) {
            toStringBudgetWindowStart = nanoNow;
            toStringTimeUsedThisSecond = 0;
        }
        if (toStringTimeUsedThisSecond >= toStringBudgetNanos) {
            droppedByRateLimit++;
            return;
        }

        Object value = record.getValue();
        String dataType = value != null ? value.getClass().getName() : "null";

        long beforeToString = System.nanoTime();
        String data = convertToString(value);
        toStringTimeUsedThisSecond += (System.nanoTime() - beforeToString);

        boolean truncated = false;

        if (data.length() > samplingConfig.getMaxRecordLength()) {
            data = data.substring(0, samplingConfig.getMaxRecordLength());
            truncated = true;
        }

        Long recordTimestamp = record.hasTimestamp() ? record.getTimestamp() : null;
        SampledRecord sampledRecord =
                new SampledRecord(now, recordTimestamp, data, dataType, truncated, sideOutputName);

        if (!sampleBuffer.tryAdd(sampledRecord)) {
            droppedByContention++;
        }
    }

    private <X> void sampleSideOutputRecord(OutputTag<X> outputTag, StreamRecord<X> record) {
        String sideOutputName = outputTag != null ? outputTag.getId() : null;
        sampleRecord(record, sideOutputName);
    }

    private boolean checkRateLimit(long now) {
        if (now - currentSecondStart >= 1000) {
            currentSecondStart = now;
            samplesThisSecond = 0;
        }
        return ++samplesThisSecond <= samplingConfig.getMaxSampleRate();
    }

    private String convertToString(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[]) {
            return "[Binary data: " + ((byte[]) value).length + " bytes]";
        }
        if (!hasCustomToString(value.getClass())) {
            return "[" + value.getClass().getSimpleName() + ": toString() not overridden]";
        }
        try {
            return value.toString();
        } catch (Exception e) {
            LOG.debug("Failed to convert record to string", e);
            return "[" + value.getClass().getSimpleName() + ": toString() threw exception]";
        }
    }

    private static boolean hasCustomToString(Class<?> clazz) {
        return HAS_CUSTOM_TOSTRING.get(clazz);
    }
}
