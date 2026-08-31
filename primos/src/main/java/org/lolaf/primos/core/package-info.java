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
 * The Raft role state machine, and nothing else.
 *
 * <p><strong>Internal.</strong> Nothing here is public API. These types carry no compatibility
 * guarantee and may change, move or be deleted in any release, so an application should not compile
 * against them. Use {@link org.lolaf.primos.PrimosNode}.
 *
 * <p>They are {@code public} only because {@code primos-witness} is a separate artifact driving the
 * same state machine, and primos ships an {@code Automatic-Module-Name} rather than a module
 * descriptor — so there is no way to export a package to one consumer without exporting it to
 * everyone. Read the modifier as a limitation of the packaging, not as an invitation.
 *
 * <p>{@link org.lolaf.primos.core.ElectionNode} is the whole package: terms, votes, randomised
 * election timeouts, heartbeats, CheckQuorum, PreVote, the graceful handover and the departure
 * announcement. It owns no thread. {@code runOnce} is called by the node's own loop in production
 * and by the deterministic simulation in tests, so both drive identical code — which is what makes
 * the simulation evidence about the real thing rather than about a copy of it.
 */
package org.lolaf.primos.core;
