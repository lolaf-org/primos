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

/**
 * The only source of time in primos.
 *
 * <p>Nothing anywhere else may call {@link System#nanoTime()} directly, for two reasons. The first
 * is correctness: {@link System#currentTimeMillis()} jumps backwards under NTP correction, which
 * would freeze every timeout in the cluster or fire them all at once, so time must be monotonic
 * and this interface is what makes that a rule rather than a convention. The second is testing:
 * the deterministic simulation supplies a virtual implementation and advances it by hand, which is
 * what lets a whole cluster run reproducibly on one thread.
 *
 * <p>Waiting is deliberately not part of this interface. A node blocks in exactly one place, its
 * selector, and the simulation replaces that blocking point rather than this clock.
 */
@FunctionalInterface
public interface MonotonicClock {

    /** The real clock, backed by {@link System#nanoTime()}. */
    MonotonicClock SYSTEM = System::nanoTime;

    /**
     * Nanoseconds from an arbitrary origin. Only differences are meaningful, and the value never
     * decreases.
     */
    long nanoTime();
}
