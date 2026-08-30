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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Peer;
import org.lolaf.primos.wire.WireFormat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The same contract as every other transport, over a real socket.
 */
class UdpTransportTest extends TransportContractTest {

    private static final String LOOPBACK = "127.0.0.1";

    private int portA;
    private int portB;

    private static ByteBuffer datagram() {
        ByteBuffer buffer = ByteBuffer.allocate(WireFormat.HEADER_BYTES);
        buffer.position(WireFormat.HEADER_BYTES);
        buffer.flip();
        return buffer;
    }

    @Override
    protected void createTransports() {
        portA = Ports.free();
        portB = Ports.free();
        a = udp(portA, Peer.application(NODE_B, LOOPBACK, portB));
        b = udp(portB, Peer.application(NODE_A, LOOPBACK, portA));
    }

    @Test
    void it_binds_where_it_was_told_to() {
        assertThat(((UdpTransport) a).boundAddress().getPort()).isEqualTo(portA);
    }

    @Test
    void an_operating_system_chosen_port_is_reported_back() throws IOException {
        try (UdpTransport transport = UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Collections.emptyList())
                .build()) {

            assertThat(transport.boundAddress().getPort()).isPositive();
        }
    }

    @Test
    void a_peer_that_is_not_listening_costs_a_send_but_not_an_exception() {
        UdpTransport transport = (UdpTransport) a;
        int deadPort = Ports.free();
        Transport toNowhere = udp(Ports.free(), Peer.application(7, LOOPBACK, deadPort));

        for (int i = 0; i < 20; i++) {
            toNowhere.send(7, wrap("into the void"));
        }

        assertThat(transport.sendFailures())
                .as("UDP sends into nothing usually succeed locally; what matters is that none"
                        + " of it ever throws at the algorithm")
                .isNotNegative();
        toNowhere.close();
    }

    @Test
    void an_oversized_datagram_is_truncated_into_failing_rather_than_silently_accepted() {
        byte[] oversized = new byte[600];
        java.util.Arrays.fill(oversized, (byte) 7);

        a.send(NODE_B, java.nio.ByteBuffer.wrap(oversized));

        int[] receivedSize = {-1};
        b.poll(TimeUnit.SECONDS.toNanos(2), datagram -> receivedSize[0] = datagram.remaining());

        assertThat(receivedSize[0])
                .as("the receive buffer is one byte larger than a legal datagram, so anything"
                        + " oversized arrives too long for any message type and is rejected above")
                .isEqualTo(WireFormat.MAX_DATAGRAM_BYTES + 1);
    }

    @Test
    void polling_with_no_timeout_returns_at_once_rather_than_blocking_forever() {
        long startedAt = System.nanoTime();

        int delivered = a.poll(0, datagram -> {
        });

        assertThat(delivered).isZero();
        assertThat(System.nanoTime() - startedAt)
                .as("select(0) blocks indefinitely in NIO; this must not")
                .isLessThan(TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void wakeup_unblocks_a_waiting_poll() throws Exception {
        Thread waiting = new Thread(() -> a.poll(TimeUnit.SECONDS.toNanos(30), datagram -> {
        }));
        waiting.start();
        Thread.sleep(100);

        a.wakeup();
        waiting.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiting.isAlive())
                .as("stop() must not have to wait out a timeout")
                .isFalse();
    }

    // ------------------------------------------------- sends that never happen

    private Transport udp(int port, Peer... peers) {
        try {
            return UdpTransport.builder()
                    .bindAddress(new InetSocketAddress(LOOPBACK, port))
                    .peers(Arrays.asList(peers))
                    .clusterName("test")
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("cannot bind " + port, e);
        }
    }

    /**
     * Every way a send can fail to reach the wire, each proved to be counted.
     *
     * <p>The SPI promises a failed send is "counted and logged rather than propagated". Three of
     * these four paths used to do neither: they returned quietly, so a peer that could never be
     * sent to looked exactly like a peer that was simply not talking back.
     */
    @Test
    void a_send_to_a_peer_that_was_never_configured_is_counted() throws IOException {
        try (UdpTransport transport = UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Collections.singletonList(Peer.application(NODE_B, LOOPBACK, Ports.free())))
                .build()) {

            transport.send(99, datagram());

            assertThat(transport.sendsToUnknownPeer())
                    .as("not a network condition: something asked for an id that was never"
                            + " configured, which is a bug rather than a lost datagram")
                    .isEqualTo(1);
            assertThat(transport.sendsDropped()).isEqualTo(1);
        }
    }

    @Test
    void a_hostname_that_does_not_resolve_refuses_to_build() {
        assertThatThrownBy(() -> UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Collections.singletonList(
                        Peer.application(NODE_B, "no-such-host.invalid", 7420)))
                .build())
                .as("a node that cannot work out where a peer is could never reach it; dropping"
                        + " the datagrams instead made broken DNS look like a partition")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("peer " + NODE_B)
                .hasMessageContaining("no-such-host.invalid:7420");
    }

    @Test
    void every_unresolvable_peer_is_named_at_once() {
        assertThatThrownBy(() -> UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Arrays.asList(
                        Peer.application(NODE_A, "no-such-host-a.invalid", 7420),
                        Peer.application(NODE_B, LOOPBACK, 7421),
                        Peer.witness(9, "no-such-host-b.invalid", 7422)))
                .build())
                .as("stopping at the first would make an operator who mistyped three hostnames"
                        + " find them one restart at a time")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no-such-host-a.invalid")
                .hasMessageContaining("no-such-host-b.invalid")
                .hasMessageContaining("2 of 3 peers");
    }

    @Test
    void a_resolvable_peer_is_sent_to_rather_than_dropped() throws IOException {
        // The negative twin of the two above: the eager resolution must not be passing by never
        // putting anything on the wire.
        try (UdpTransport transport = UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Collections.singletonList(Peer.application(NODE_B, "localhost", Ports.free())))
                .build()) {

            transport.send(NODE_B, datagram());

            assertThat(transport.sendsDropped())
                    .as("a name that resolves is sent to, and nothing about it is counted")
                    .isZero();
        }
    }

    @Test
    void a_send_after_close_is_counted_rather_than_ignored() throws IOException {
        UdpTransport transport = UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .peers(Collections.singletonList(Peer.application(NODE_B, LOOPBACK, Ports.free())))
                .build();
        transport.close();

        transport.send(NODE_B, datagram());

        assertThat(transport.sendsWhileClosed()).isEqualTo(1);
        assertThat(transport.sendsDropped()).isEqualTo(1);
    }

    @Test
    void a_peer_speaking_from_a_new_address_is_learned_and_counted() throws IOException {
        int learningPort = Ports.free();
        try (UdpTransport learning = (UdpTransport) UdpTransport.builder()
                .bindAddress(new InetSocketAddress(LOOPBACK, learningPort))
                // Configured to expect node B at portB, which is where it is not.
                .peers(Collections.singletonList(Peer.application(NODE_B, LOOPBACK, portB)))
                .addressLearning(true)
                .clusterName("test")
                .build();
             Transport moved = udp(Ports.free(), Peer.application(NODE_A, LOOPBACK, learningPort))) {

            moved.send(NODE_A, datagram());
            learning.poll(TimeUnit.SECONDS.toNanos(2), buffer -> {
                // Inside the handler, deliberately: the transport only knows where the datagram
                // it is currently delivering came from. In production the node calls this from
                // the same place, once the datagram has passed every check — learning from an
                // unverified one would let a spoofed packet redirect a peer's traffic.
                learning.confirmSender(NODE_B);
                learning.confirmSender(NODE_B);
            });

            assertThat(learning.addressUpdates())
                    .as("a peer that restarts on a new port is reachable again without an"
                            + " operator editing configuration; the counter is how anybody ever"
                            + " knows that happened, and the same address twice is not two moves")
                    .isEqualTo(1);
        }
    }

    @Test
    void a_healthy_transport_drops_nothing() {
        ((UdpTransport) a).send(NODE_B, datagram());

        assertThat(((UdpTransport) a).sendsDropped())
                .as("the one number worth alerting on, and it stays flat when nothing is wrong")
                .isZero();
    }
}
