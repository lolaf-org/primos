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
 * The public API — the entry point, and the only package most applications import.
 *
 * <p>Start at {@link org.lolaf.primos.PrimosNode}. Its builder takes the cluster's membership and
 * timings, {@link org.lolaf.primos.PrimosNode#start()} joins, and
 * {@link org.lolaf.primos.PrimosNode#roleAndTerm()} or a {@link org.lolaf.primos.PrimosListener}
 * answers who is in charge. Read the safe-usage note on {@code PrimosNode} before guarding an
 * external side effect with that answer: the role alone is never a sufficient guard, and the term
 * is what makes it one.
 *
 * <p>Three neighbouring packages are public and supported too, because an application may
 * legitimately supply its own implementation: {@link org.lolaf.primos.transport},
 * {@link org.lolaf.primos.store} and {@link org.lolaf.primos.time}.
 * {@link org.lolaf.primos.core} and {@link org.lolaf.primos.wire} are implementation and say so
 * themselves.
 *
 * <p>The node's own implementation lives here as well, package-private. That is deliberate rather
 * than accidental: keeping it in {@code core} meant a public factory whose only reason to exist was
 * to cross the package boundary, and it read as a second and more powerful way to build a node than
 * the builder. Moving the implementation to the API deleted it. Do not move it back.
 */
package org.lolaf.primos;
