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

/**
 * The one source of time, {@link org.lolaf.primos.time.MonotonicClock}.
 *
 * <p>Public and supported, though most applications never touch it: the node defaults to the real
 * clock. It is an interface rather than a direct call to {@link java.lang.System#nanoTime()} for
 * two reasons — a wall clock that jumps backwards under NTP correction would freeze every timeout
 * in the cluster or fire them all at once, and the deterministic simulation needs to advance time
 * by hand to run a whole cluster reproducibly on one thread.
 *
 * <p>A test greps the main sources for direct clock access, so the rule holds with no exceptions to
 * remember. Waiting is deliberately not part of the interface: a node blocks in exactly one place,
 * its selector, and the simulation replaces that blocking point rather than this clock.
 */
package org.lolaf.primos.time;
