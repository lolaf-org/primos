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
package org.lolaf.primos.sim;

/**
 * An invariant broke.
 *
 * <p>Carries the seed, because a failure nobody can reproduce is a rumour. The message is written
 * to be pasted straight into a regression test.
 */
public final class SimulationFailure extends AssertionError {

    private static final long serialVersionUID = 1L;

    private final long seed;

    public SimulationFailure(long seed, long nowNanos, String invariant, String detail, String trace) {
        super("invariant '" + invariant + "' broke at t=" + nowNanos + "ns\n"
                + "  " + detail + "\n"
                + "  reproduce with seed " + seed + "L\n"
                + "  recent history:\n" + trace);
        this.seed = seed;
    }

    /** The seed that produced this run. Check it in as a regression. */
    public long seed() {
        return seed;
    }
}
