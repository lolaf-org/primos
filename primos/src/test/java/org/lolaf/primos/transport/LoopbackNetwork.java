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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * An in-JVM network that really blocks, for tests that need real threads.
 *
 * <p>The deterministic simulation deliberately never blocks — the harness owns time there — which
 * makes it useless for exercising a node that owns a thread and waits on its socket. This fills
 * that gap without needing a real port: {@link Transport#poll} waits on a queue exactly as the UDP
 * transport waits on a datagram channel, and {@link Transport#wakeup()} makes it return promptly
 * so {@code stop()} does not have to wait out a timeout.
 */
public final class LoopbackNetwork {

    private static final byte[] WAKEUP = new byte[0];

    private final Map<Integer, LinkedBlockingQueue<byte[]>> mailboxes = new ConcurrentHashMap<>();

    /** A transport for one node. */
    public Transport transportFor(int nodeId) {
        mailboxes.computeIfAbsent(nodeId, id -> new LinkedBlockingQueue<>());
        return new LoopbackTransport(nodeId);
    }

    private final class LoopbackTransport implements Transport {

        private final int nodeId;
        private volatile boolean closed;
        private volatile long sendsDropped;

        private LoopbackTransport(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public void send(int peerId, ByteBuffer datagram) {
            if (closed) {
                sendsDropped++;
                return;
            }
            LinkedBlockingQueue<byte[]> mailbox = mailboxes.get(peerId);
            if (mailbox == null) {
                // Sending into the void, exactly as a datagram to a host that is not listening.
                // Counted so that a test naming a peer that was never registered fails on the
                // counter rather than on a silence it has to work backwards from.
                sendsDropped++;
                return;
            }
            byte[] copy = new byte[datagram.remaining()];
            datagram.duplicate().get(copy);
            mailbox.offer(copy);
        }

        @Override
        public int poll(long timeoutNanos, DatagramHandler handler) {
            LinkedBlockingQueue<byte[]> mailbox = mailboxes.get(nodeId);
            if (closed || mailbox == null) {
                return 0;
            }
            int delivered = 0;
            try {
                byte[] first = mailbox.poll(Math.max(0L, timeoutNanos), TimeUnit.NANOSECONDS);
                if (first == null) {
                    return 0;
                }
                delivered += deliver(first, handler);
                byte[] next;
                while ((next = mailbox.poll()) != null) {
                    delivered += deliver(next, handler);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return delivered;
        }

        private int deliver(byte[] bytes, DatagramHandler handler) {
            if (bytes.length == 0) {
                return 0;
            }
            handler.onDatagram(ByteBuffer.wrap(bytes));
            return 1;
        }

        @Override
        public long sendsDropped() {
            return sendsDropped;
        }

        @Override
        public void wakeup() {
            LinkedBlockingQueue<byte[]> mailbox = mailboxes.get(nodeId);
            if (mailbox != null) {
                mailbox.offer(WAKEUP);
            }
        }

        @Override
        public void close() {
            closed = true;
            wakeup();
        }

        @Override
        public String toString() {
            return "LoopbackTransport(" + nodeId + ")";
        }
    }
}
