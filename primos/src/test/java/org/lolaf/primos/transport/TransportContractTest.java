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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The behaviour every {@link Transport} must have, real or simulated.
 *
 * <p>Subclassed once per implementation and never copied. This is the contract the simulation
 * relies on being a faithful stand-in for a socket: if the simulated transport quietly behaved
 * better than UDP, every test built on it would be testing a network that does not exist.
 */
abstract class TransportContractTest {

    protected static final int NODE_A = 1;
    protected static final int NODE_B = 2;
    protected static final long POLL_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);

    protected Transport a;
    protected Transport b;

    /** Creates two transports that can reach each other, assigning {@link #a} and {@link #b}. */
    protected abstract void createTransports();

    @BeforeEach
    void openTransports() {
        createTransports();
    }

    @AfterEach
    void closeTransports() {
        a.close();
        b.close();
    }

    @Test
    void bytes_sent_arrive_unchanged() {
        a.send(NODE_B, wrap("hello"));

        assertThat(receiveOne(b)).isEqualTo("hello");
    }

    @Test
    void a_datagram_arrives_whole_or_not_at_all() {
        byte[] payload = new byte[400];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        a.send(NODE_B, ByteBuffer.wrap(payload));

        List<byte[]> received = receive(b);
        assertThat(received).hasSize(1);
        assertThat(received.get(0)).isEqualTo(payload);
    }

    @Test
    void the_sender_may_reuse_its_buffer_the_moment_send_returns() {
        ByteBuffer reused = ByteBuffer.allocate(16);
        reused.put("first".getBytes(StandardCharsets.UTF_8)).flip();

        a.send(NODE_B, reused);

        reused.clear();
        reused.put("SCRIBBLE".getBytes(StandardCharsets.UTF_8)).flip();

        assertThat(receiveOne(b))
                .as("the transport must consume the bytes before returning, because a node holds"
                        + " exactly one send buffer for its whole life")
                .isEqualTo("first");
    }

    @Test
    void messages_flow_in_both_directions() {
        a.send(NODE_B, wrap("ping"));
        assertThat(receiveOne(b)).isEqualTo("ping");

        b.send(NODE_A, wrap("pong"));
        assertThat(receiveOne(a)).isEqualTo("pong");
    }

    @Test
    void many_datagrams_all_arrive_on_a_healthy_network() {
        for (int i = 0; i < 50; i++) {
            a.send(NODE_B, wrap("message-" + i));
        }

        List<String> received = new ArrayList<>();
        while (received.size() < 50) {
            int delivered = b.poll(POLL_TIMEOUT_NANOS,
                    datagram -> received.add(text(datagram)));
            if (delivered == 0) {
                break;
            }
        }

        assertThat(received).hasSize(50);
        assertThat(received).contains("message-0", "message-49");
    }

    @Test
    void polling_an_idle_transport_delivers_nothing() {
        List<String> received = new ArrayList<>();

        assertThat(a.poll(0, datagram -> received.add(text(datagram)))).isZero();
        assertThat(received).isEmpty();
    }

    @Test
    void sending_to_a_peer_that_is_not_there_is_not_an_error() {
        assertThatCode(() -> a.send(9999, wrap("into the void")))
                .as("UDP sends fail routinely; the algorithm's timers are the recovery mechanism")
                .doesNotThrowAnyException();
    }

    // ------------------------------------------------- saying what was dropped

    /**
     * A transport may drop anything, and must be able to say how much of what it dropped.
     *
     * <p>A send that returns without either sending or recording anything is indistinguishable
     * from one that worked, so broken configuration presented as a silent partition. The rule is
     * in the SPI rather than in each implementation's own vocabulary precisely so it can be checked
     * once, here, against every implementation there will ever be.
     *
     * <p>What is asserted is deliberately only what every transport can honestly mean. Sending to
     * an id the transport has no address for is <em>not</em> here: it is a real drop for the UDP
     * and loopback transports, which have address tables, and a meaningless question for the
     * simulated one, which has none. That case keeps its own test beside the implementation whose
     * counter it is.
     */
    @Test
    void a_new_transport_has_dropped_nothing() {
        assertThat(a.sendsDropped()).isZero();
    }

    @Test
    void a_send_that_reaches_a_peer_is_not_counted_as_dropped() {
        a.send(NODE_B, wrap("delivered"));

        assertThat(a.sendsDropped())
                .as("flat on a healthy transport, which is the whole of what makes it alertable")
                .isZero();
    }

    @Test
    void a_send_after_close_is_counted_rather_than_silently_swallowed() {
        a.close();

        a.send(NODE_B, wrap("too late"));

        assertThat(a.sendsDropped())
                .as("expected during shutdown and harmless, but still a datagram that never left")
                .isEqualTo(1);
    }

    @Test
    void closing_twice_is_harmless() {
        assertThatCode(() -> {
            a.close();
            a.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void a_closed_transport_delivers_nothing_further() {
        a.send(NODE_B, wrap("before close"));
        b.close();

        assertThat(b.poll(0, datagram -> {
            throw new AssertionError("a closed transport delivered " + text(datagram));
        })).isZero();
    }

    @Test
    void wakeup_is_safe_to_call_even_when_nothing_is_blocked() {
        assertThatCode(() -> a.wakeup()).doesNotThrowAnyException();
    }

    protected static ByteBuffer wrap(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    protected static String text(ByteBuffer datagram) {
        byte[] bytes = new byte[datagram.remaining()];
        datagram.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String receiveOne(Transport transport) {
        List<byte[]> received = receive(transport);
        assertThat(received).hasSize(1);
        return new String(received.get(0), StandardCharsets.UTF_8);
    }

    private List<byte[]> receive(Transport transport) {
        List<byte[]> received = new ArrayList<>();
        transport.poll(POLL_TIMEOUT_NANOS, datagram -> {
            byte[] bytes = new byte[datagram.remaining()];
            datagram.duplicate().get(bytes);
            received.add(bytes);
        });
        return received;
    }
}
