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
 * A property that must hold at every instant of a simulated run.
 *
 * <p>Checked after every scheduler step rather than at the end, because the interesting failures
 * are transient: two leaders for fifty milliseconds is exactly as fatal as two leaders forever,
 * and a check that only runs at the end would see a tidy cluster and pass.
 *
 * <p>Implementations are stateful, since most of these are about how state changed rather than
 * what it is.
 */
public interface Invariant {

    /** Short name, used in failure messages. */
    String name();

    /**
     * Examines the cluster as it is now.
     *
     * @throws AssertionError if the property no longer holds. The simulation catches it and adds
     *                        the seed and recent history.
     */
    void check(Simulation simulation);
}
