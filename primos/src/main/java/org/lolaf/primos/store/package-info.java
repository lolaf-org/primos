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
 * Where a node's term and vote survive a restart: the {@link org.lolaf.primos.store.StateStore} SPI
 * with a file implementation and an in-memory one.
 *
 * <p>Public and supported. {@link org.lolaf.primos.store.FileStateStore} is the production choice —
 * temp file, fsync, atomic rename, a CRC over the record and an exclusive lock so two processes
 * cannot vote from one identity. It refuses to start on a corrupt file rather than treating it as a
 * fresh node, because that fallback is precisely how a vote is forgotten and a second leader
 * elected.
 *
 * <p>{@link org.lolaf.primos.store.MemoryStateStore} is for tests and the simulation. It reports
 * {@link org.lolaf.primos.store.StateStore#isDurable()} false and callers apply their own policy: a
 * node warns and continues, the witness server refuses outright.
 *
 * <p>This is the smallest package in primos and the one carrying the most safety. Two fields, and
 * they are written together or not at all.
 */
package org.lolaf.primos.store;
