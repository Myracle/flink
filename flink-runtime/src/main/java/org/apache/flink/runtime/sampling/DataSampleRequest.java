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
import java.time.Duration;

/** Request to start a data sampling round on a set of tasks. */
@Internal
public class DataSampleRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int roundId;
    private final Duration samplingWindow;
    private final int maxBufferCapacity;

    public DataSampleRequest(int roundId, Duration samplingWindow, int maxBufferCapacity) {
        this.roundId = roundId;
        this.samplingWindow = samplingWindow;
        this.maxBufferCapacity = maxBufferCapacity;
    }

    public int getRoundId() {
        return roundId;
    }

    public Duration getSamplingWindow() {
        return samplingWindow;
    }

    public int getMaxBufferCapacity() {
        return maxBufferCapacity;
    }
}
