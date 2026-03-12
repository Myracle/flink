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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Immutable configuration for data sampling on a single task. */
@Internal
public class SamplingConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int maxSampleRate;
    private final int maxRecordLength;
    private final long maxResponseBytes;

    public SamplingConfig(int maxSampleRate, int maxRecordLength, long maxResponseBytes) {
        Preconditions.checkArgument(maxSampleRate > 0, "maxSampleRate must be positive");
        Preconditions.checkArgument(maxRecordLength > 0, "maxRecordLength must be positive");
        Preconditions.checkArgument(maxResponseBytes > 0, "maxResponseBytes must be positive");
        this.maxSampleRate = maxSampleRate;
        this.maxRecordLength = maxRecordLength;
        this.maxResponseBytes = maxResponseBytes;
    }

    public static SamplingConfig fromConfiguration(Configuration configuration) {
        return new SamplingConfig(
                configuration.get(RestOptions.DATA_SAMPLING_MAX_SAMPLE_RATE),
                configuration.get(RestOptions.DATA_SAMPLING_MAX_RECORD_LENGTH),
                configuration.get(RestOptions.DATA_SAMPLING_MAX_RESPONSE_BYTES));
    }

    public int getMaxSampleRate() {
        return maxSampleRate;
    }

    public int getMaxRecordLength() {
        return maxRecordLength;
    }

    public long getMaxResponseBytes() {
        return maxResponseBytes;
    }
}
