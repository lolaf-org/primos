/*
 * Copyright © 2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.primos.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class VirtualClockTest {

    @Test
    void starts_at_zero_and_advances_by_exactly_what_it_is_given() {
        VirtualClock clock = new VirtualClock();
        assertThat(clock.nanoTime()).isZero();

        clock.advance(Duration.ofMillis(300));

        assertThat(clock.nanoTime()).isEqualTo(Duration.ofMillis(300).toNanos());
    }

    @Test
    void advancing_accumulates() {
        VirtualClock clock = new VirtualClock(1_000L);

        clock.advanceNanos(5L);
        clock.advanceNanos(5L);

        assertThat(clock.nanoTime()).isEqualTo(1_010L);
    }

    @Test
    void refuses_to_go_backwards() {
        VirtualClock clock = new VirtualClock();

        assertThatThrownBy(() -> clock.advanceNanos(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot go backwards");
    }

    @Test
    void the_system_clock_is_monotonic_and_usable_through_the_interface() {
        MonotonicClock clock = MonotonicClock.SYSTEM;

        long first = clock.nanoTime();
        long second = clock.nanoTime();

        assertThat(second).isGreaterThanOrEqualTo(first);
    }
}
