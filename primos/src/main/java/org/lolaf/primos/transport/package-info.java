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
 * How a node reaches its peers: the {@link org.lolaf.primos.transport.Transport} SPI and the UDP
 * implementation of it.
 *
 * <p>Public and supported. An application hands a transport to the node builder, and
 * {@link org.lolaf.primos.transport.UdpTransport} is the one to use unless there is a specific
 * reason not to — it resolves every peer's hostname once at {@code build()} and refuses to start on
 * a name that does not resolve, so a configuration error is reported to whoever called it rather
 * than becoming a partition nobody can diagnose.
 *
 * <p>The SPI is an abstraction rather than speculative generality: the deterministic simulation
 * supplies the second implementation, and a blocking loopback network supplies a third. All of them
 * answer the same contract test, which is what keeps the contract honest.
 *
 * <p>It is message-oriented and connectionless on purpose, and a transport may drop anything —
 * but it must be able to say how much of what it dropped, which is why
 * {@link org.lolaf.primos.transport.Transport#sendsDropped()} is on the SPI rather than left to
 * each implementation to spell its own way.
 */
package org.lolaf.primos.transport;
