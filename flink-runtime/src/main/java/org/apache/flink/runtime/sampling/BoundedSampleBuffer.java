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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A bounded buffer for collecting sampled records during a data sampling round.
 *
 * <p>Not thread-safe. All methods must be called from the same thread (the mailbox thread in
 * Flink's streaming runtime).
 *
 * @param <T> the type of elements in the buffer
 */
@Internal
public class BoundedSampleBuffer<T> {

    private ArrayList<T> buffer;
    private int capacity;

    public BoundedSampleBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new ArrayList<>(capacity);
    }

    /**
     * Adds an item to the buffer if it is not full.
     *
     * @return {@code true} if the item was added, {@code false} if the buffer is at capacity
     */
    public boolean tryAdd(T item) {
        if (buffer.size() >= capacity) {
            return false;
        }
        buffer.add(item);
        return true;
    }

    /** Drains all items from the buffer and returns them as an unmodifiable list. */
    public List<T> drainAndClear() {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = Collections.unmodifiableList(buffer);
        buffer = new ArrayList<>(capacity);
        return result;
    }

    /** Resets the buffer with a new capacity, discarding any existing items. */
    public void reset(int newCapacity) {
        this.capacity = newCapacity;
        this.buffer = new ArrayList<>(newCapacity);
    }
}
