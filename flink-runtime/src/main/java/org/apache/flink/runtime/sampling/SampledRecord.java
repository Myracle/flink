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

import javax.annotation.Nullable;

import java.io.Serializable;

/** A single sampled record captured during a data sampling round. */
@Internal
public class SampledRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long sampleTimestamp;
    @Nullable private final Long recordTimestamp;
    private final String data;
    private final String dataType;
    private final boolean truncated;
    @Nullable private final String sideOutputName;

    public SampledRecord(
            long sampleTimestamp,
            @Nullable Long recordTimestamp,
            String data,
            String dataType,
            boolean truncated,
            @Nullable String sideOutputName) {
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
}
