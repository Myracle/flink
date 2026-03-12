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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Result of a data sampling round from a single output. */
@Internal
public class SamplingRoundResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int roundId;
    private final List<SampledRecord> records;
    private final int droppedByContention;
    private final int droppedByRateLimit;
    private final SampleStatus status;

    public SamplingRoundResult(
            int roundId,
            List<SampledRecord> records,
            int droppedByContention,
            int droppedByRateLimit,
            SampleStatus status) {
        this.roundId = roundId;
        this.records = records;
        this.droppedByContention = droppedByContention;
        this.droppedByRateLimit = droppedByRateLimit;
        this.status = status;
    }

    public static SamplingRoundResult empty(int roundId) {
        return new SamplingRoundResult(
                roundId, Collections.emptyList(), 0, 0, SampleStatus.NO_DATA);
    }

    public static SamplingRoundResult failed(int roundId, SampleStatus status) {
        return new SamplingRoundResult(roundId, Collections.emptyList(), 0, 0, status);
    }

    public int getRoundId() {
        return roundId;
    }

    public List<SampledRecord> getRecords() {
        return records;
    }

    public int getDroppedByContention() {
        return droppedByContention;
    }

    public int getDroppedByRateLimit() {
        return droppedByRateLimit;
    }

    public SampleStatus getStatus() {
        return status;
    }
}
