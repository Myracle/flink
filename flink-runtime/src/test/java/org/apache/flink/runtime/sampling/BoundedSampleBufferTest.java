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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BoundedSampleBuffer}. */
class BoundedSampleBufferTest {

    @Test
    void testAddAndDrain() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(10);
        assertThat(buffer.tryAdd("a")).isTrue();
        assertThat(buffer.tryAdd("b")).isTrue();

        List<String> drained = buffer.drainAndClear();
        assertThat(drained).containsExactly("a", "b");
    }

    @Test
    void testDrainEmptyBuffer() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(10);
        List<String> drained = buffer.drainAndClear();
        assertThat(drained).isEmpty();
    }

    @Test
    void testCapacityEnforced() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(2);
        assertThat(buffer.tryAdd("a")).isTrue();
        assertThat(buffer.tryAdd("b")).isTrue();
        assertThat(buffer.tryAdd("c")).isFalse();
    }

    @Test
    void testDrainClearsBuffer() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(10);
        buffer.tryAdd("a");
        buffer.drainAndClear();

        // Buffer should be empty after drain
        List<String> drained = buffer.drainAndClear();
        assertThat(drained).isEmpty();

        // Can add again
        assertThat(buffer.tryAdd("b")).isTrue();
    }

    @Test
    void testResetChangesCapacity() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(2);
        buffer.tryAdd("a");
        buffer.tryAdd("b");

        // Reset with larger capacity
        buffer.reset(5);
        List<String> drained = buffer.drainAndClear();
        assertThat(drained).isEmpty(); // reset clears buffer

        // New capacity allows 5 items
        for (int i = 0; i < 5; i++) {
            assertThat(buffer.tryAdd("item-" + i)).isTrue();
        }
        assertThat(buffer.tryAdd("overflow")).isFalse();
    }

    @Test
    void testZeroCapacity() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(0);
        assertThat(buffer.tryAdd("a")).isFalse();
        assertThat(buffer.drainAndClear()).isEmpty();
    }

    @Test
    void testDrainedListIsUnmodifiable() {
        BoundedSampleBuffer<String> buffer = new BoundedSampleBuffer<>(10);
        buffer.tryAdd("a");
        List<String> drained = buffer.drainAndClear();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> drained.add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
