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
package org.lolaf.primos.transport;

import java.nio.ByteBuffer;

/**
 * How a node exchanges messages with its peers.
 *
 * <p>Message-oriented rather than stream-oriented, and deliberately without connections. UDP has
 * none, and Raft detects failure by timeout rather than by connection state, so a connect and
 * disconnect callback would be machinery nothing wants. That same shape is what the deterministic
 * simulation needs — dropping, delaying and reordering are all per message — so one SPI serves the
 * real transport and the simulated one without either compromising.
 *
 * <p><strong>Sending is fire-and-forget and may silently fail.</strong> There is no acknowledgement
 * and no retry here, because Raft is already a retry protocol: a lost vote request is retried by
 * the next election timeout, a lost heartbeat by the next heartbeat interval. A transport that
 * retried would be duplicating work the algorithm does anyway, and doing it with less information.
 *
 * <p>Implementations are owned by one node thread and need not be thread-safe, with the single
 * exception of {@link #wakeup()}.
 */
public interface Transport extends AutoCloseable {

    /**
     * Sends a datagram to a peer, if it can.
     *
     * <p>The buffer's contents are consumed before this returns, so the caller may reuse it
     * immediately — which the node does, holding one send buffer for its whole life.
     *
     * <p>Errors are not thrown. UDP sends fail routinely, and an ICMP port-unreachable can surface
     * long after the fact; the algorithm's timers are the recovery mechanism, so a failure here is
     * counted and logged rather than propagated. Letting it reach the node thread's catch-all would
     * turn an unremarkable network event into a failed node.
     */
    void send(int peerId, ByteBuffer datagram);

    /**
     * Waits for datagrams and delivers whatever arrived, returning how many.
     *
     * <p>This is the node's single blocking point: one call that waits for both incoming messages
     * and the next timer deadline, which is what lets a node run on exactly one thread.
     *
     * @param timeoutNanos how long to wait when nothing has arrived. Zero returns immediately.
     *                     The simulation ignores it, because there the harness owns time.
     */
    int poll(long timeoutNanos, DatagramHandler handler);

    /**
     * How many datagrams this transport never handed to the network, for any reason.
     *
     * <p>Part of the contract rather than a convenience, and the reason is a bug this project
     * already had: {@link #send} promises a failure is "counted and logged rather than propagated",
     * and three separate implementations had paths that returned without doing either — a peer that
     * could never be sent to looked exactly like a peer that was simply not answering. A transport
     * may drop anything; it must be able to say how much of what it dropped.
     *
     * <p><strong>This counts what never left, not what was lost on the way.</strong> Datagrams the
     * network discarded after this transport handed them over are invisible here, exactly as they
     * are to a UDP socket — the simulated network's seeded loss and its partitions are network
     * behaviour and are counted there instead. What belongs here is a closed transport, an id the
     * transport has no address for, and a socket that refused the write.
     *
     * <p>Monotonic, and flat on a healthy transport, which is what makes it worth alerting on.
     */
    long sendsDropped();

    /**
     * Makes a blocked {@link #poll} return promptly. The one method other threads may call, so
     * {@code stop()} does not have to wait out a timeout.
     */
    void wakeup();

    /**
     * Tells the transport that the datagram it just delivered genuinely came from this peer.
     *
     * <p>The hook exists for address learning: a peer that restarts on a new address can only be
     * reached again if its endpoint is updated from traffic. Doing that on an unauthenticated
     * datagram would let one spoofed packet redirect a peer's traffic and partition it without
     * forging any content, so the transport is only told once the message has passed every check
     * the decoder makes.
     *
     * <p>Ignored by transports that have no addresses to learn.
     */
    default void confirmSender(int senderId) {
    }

    /**
     * Releases the socket or its equivalent. Idempotent.
     */
    @Override
    void close();
}
