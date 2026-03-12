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
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded buffer for collecting sampled records. Thread-safe with non-blocking writes via {@link
 * ReentrantLock#tryLock()} to avoid impacting the data processing hot path.
 *
 * @param <T> the type of elements in the buffer
 */
@Internal
public class BoundedSampleBuffer<T> {

    private final ReentrantLock lock = new ReentrantLock();
    private ArrayList<T> buffer;
    private int capacity;

    public BoundedSampleBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new ArrayList<>(capacity);
    }

    /**
     * Attempts to add an item to the buffer without blocking. Returns {@code false} if the lock is
     * held by another thread or the buffer is full.
     */
    public boolean tryAdd(T item) {
        if (!lock.tryLock()) {
            return false;
        }
        try {
            if (buffer.size() >= capacity) {
                return false;
            }
            buffer.add(item);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Drains all items from the buffer and clears it. Blocks until the lock is acquired. */
    public List<T> drainAndClear() {
        lock.lock();
        try {
            if (buffer.isEmpty()) {
                return Collections.emptyList();
            }
            List<T> result = Collections.unmodifiableList(buffer);
            buffer = new ArrayList<>(capacity);
            return result;
        } finally {
            lock.unlock();
        }
    }

    /** Resets the buffer with a new capacity. Takes the lock for safety. */
    public void reset(int newCapacity) {
        lock.lock();
        try {
            this.capacity = newCapacity;
            this.buffer = new ArrayList<>(newCapacity);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return buffer.size();
    }

    public boolean isFull() {
        return buffer.size() >= capacity;
    }
}
