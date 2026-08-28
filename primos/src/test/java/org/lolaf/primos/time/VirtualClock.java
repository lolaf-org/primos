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

import java.time.Duration;

/**
 * A clock the test advances by hand.
 *
 * <p>This is the foundation of the deterministic simulation: with time under the harness's
 * control, a whole cluster runs reproducibly on one thread and a partition that lasts "four
 * election timeouts" costs no wall-clock time at all.
 */
public final class VirtualClock implements MonotonicClock {

    private long nanos;

    /** A clock reading zero. */
    public VirtualClock() {
        this(0L);
    }

    /** A clock reading a chosen origin, useful for proving that only differences are used. */
    public VirtualClock(long originNanos) {
        this.nanos = originNanos;
    }

    @Override
    public long nanoTime() {
        return nanos;
    }

    /** Moves time forward. Never backwards: a monotonic clock that rewinds is not one. */
    public void advance(Duration by) {
        advanceNanos(by.toNanos());
    }

    /** Moves time forward by nanoseconds. */
    public void advanceNanos(long by) {
        if (by < 0) {
            throw new IllegalArgumentException("a monotonic clock cannot go backwards, by=" + by);
        }
        nanos += by;
    }
}
