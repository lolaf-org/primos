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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.time.VirtualClock;

class SimulatedTransportTest extends TransportContractTest {

    private VirtualClock clock;
    private SimulatedNetwork network;

    @Override
    protected void createTransports() {
        clock = new VirtualClock();
        network = new SimulatedNetwork(20260828L, clock);
        a = network.transportFor(NODE_A);
        b = network.transportFor(NODE_B);
    }

    @Test
    void a_severed_link_delivers_nothing() {
        network.sever(NODE_A, NODE_B);

        a.send(NODE_B, wrap("lost"));

        assertThat(deliveredTo(b)).isZero();
        assertThat(network.droppedByPartition()).isEqualTo(1);
    }

    @Test
    void a_partition_cuts_both_directions_but_not_within_a_side() {
        Transport c = network.transportFor(3);
        network.partition(new int[] {NODE_A, NODE_B}, new int[] {3});

        a.send(NODE_B, wrap("same side"));
        a.send(3, wrap("across"));
        c.send(NODE_A, wrap("back across"));

        assertThat(deliveredTo(b)).isEqualTo(1);
        assertThat(deliveredTo(c)).isZero();
        assertThat(deliveredTo(a)).isZero();
    }

    @Test
    void a_one_way_link_can_be_heard_without_hearing() {
        network.sever(NODE_B, NODE_A);

        a.send(NODE_B, wrap("gets through"));
        b.send(NODE_A, wrap("does not"));

        assertThat(deliveredTo(b)).isEqualTo(1);
        assertThat(deliveredTo(a)).isZero();
    }

    @Test
    void healing_restores_every_link() {
        network.isolate(NODE_A);
        a.send(NODE_B, wrap("dropped"));
        assertThat(deliveredTo(b)).isZero();

        network.heal();
        a.send(NODE_B, wrap("delivered"));
        assertThat(deliveredTo(b)).isEqualTo(1);
    }

    @Test
    void nothing_is_delivered_before_its_delay_has_elapsed() {
        network.delayRange(TimeUnit.MILLISECONDS.toNanos(50), TimeUnit.MILLISECONDS.toNanos(50));

        a.send(NODE_B, wrap("delayed"));

        assertThat(deliveredTo(b)).as("not due yet").isZero();

        clock.advance(java.time.Duration.ofMillis(49));
        assertThat(deliveredTo(b)).as("still not due").isZero();

        clock.advance(java.time.Duration.ofMillis(1));
        assertThat(deliveredTo(b)).as("due now").isEqualTo(1);
    }

    @Test
    void the_harness_can_jump_straight_to_the_next_delivery() {
        network.delayRange(TimeUnit.MILLISECONDS.toNanos(200), TimeUnit.MILLISECONDS.toNanos(200));

        a.send(NODE_B, wrap("later"));

        long next = network.nextDeliveryNanos();
        assertThat(next).isEqualTo(TimeUnit.MILLISECONDS.toNanos(200));

        clock.advanceNanos(next - clock.nanoTime());
        assertThat(deliveredTo(b)).isEqualTo(1);
        assertThat(network.nextDeliveryNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void a_wide_delay_range_reorders_messages() {
        network.delayRange(0, TimeUnit.MILLISECONDS.toNanos(100));
        for (int i = 0; i < 20; i++) {
            a.send(NODE_B, wrap("message-" + i));
        }

        clock.advance(java.time.Duration.ofMillis(200));
        StringBuilder order = new StringBuilder();
        b.poll(0, datagram -> order.append(text(datagram)).append(' '));

        assertThat(order.toString().trim().split(" "))
                .as("varying delays are what make messages arrive out of order")
                .hasSize(20)
                .isNotEqualTo(sequentialOrder());
    }

    @Test
    void equal_delays_preserve_send_order() {
        network.delayRange(TimeUnit.MILLISECONDS.toNanos(10), TimeUnit.MILLISECONDS.toNanos(10));
        for (int i = 0; i < 20; i++) {
            a.send(NODE_B, wrap("message-" + i));
        }

        clock.advance(java.time.Duration.ofMillis(20));
        StringBuilder order = new StringBuilder();
        b.poll(0, datagram -> order.append(text(datagram)).append(' '));

        assertThat(order.toString().trim().split(" ")).isEqualTo(sequentialOrder());
    }

    @Test
    void loss_discards_some_but_not_all() {
        network.lossProbability(0.5);

        for (int i = 0; i < 500; i++) {
            a.send(NODE_B, wrap("x"));
        }

        assertThat(network.droppedByLoss()).isBetween(200L, 300L);
        assertThat(deliveredTo(b) + network.droppedByLoss()).isEqualTo(500);
    }

    @Test
    void total_loss_delivers_nothing_and_total_reliability_delivers_everything() {
        network.lossProbability(1.0);
        for (int i = 0; i < 20; i++) {
            a.send(NODE_B, wrap("x"));
        }
        assertThat(deliveredTo(b)).isZero();

        network.lossProbability(0.0);
        for (int i = 0; i < 20; i++) {
            a.send(NODE_B, wrap("x"));
        }
        assertThat(deliveredTo(b)).isEqualTo(20);
    }

    @Test
    void the_same_seed_produces_the_same_run() {
        assertThat(runTrace(99L)).isEqualTo(runTrace(99L));
    }

    @Test
    void a_different_seed_produces_a_different_run() {
        assertThat(runTrace(1L)).isNotEqualTo(runTrace(2L));
    }

    @Test
    void counters_account_for_every_datagram() {
        network.lossProbability(0.3);
        network.sever(NODE_A, 3);
        network.transportFor(3);

        for (int i = 0; i < 100; i++) {
            a.send(NODE_B, wrap("x"));
            a.send(3, wrap("y"));
        }

        assertThat(network.sent()).isEqualTo(200);
        assertThat(network.droppedByPartition()).isEqualTo(100);
        assertThat(network.inFlight() + network.dropped()).isEqualTo(network.sent());
    }

    /**
     * The same seed must produce a byte-identical run, or a checked-in regression seed proves
     * nothing and a failure cannot be replayed.
     */
    private static String runTrace(long seed) {
        VirtualClock clock = new VirtualClock();
        SimulatedNetwork network = new SimulatedNetwork(seed, clock);
        Transport from = network.transportFor(1);
        Transport to = network.transportFor(2);
        network.lossProbability(0.25);
        network.delayRange(0, TimeUnit.MILLISECONDS.toNanos(100));

        StringBuilder trace = new StringBuilder();
        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 5; i++) {
                from.send(2, wrap("r" + round + "-" + i));
            }
            clock.advance(java.time.Duration.ofMillis(30));
            to.poll(0, datagram -> trace.append(text(datagram)).append(','));
        }
        return trace.toString();
    }

    private static String[] sequentialOrder() {
        String[] expected = new String[20];
        Arrays.setAll(expected, i -> "message-" + i);
        return expected;
    }

    private int deliveredTo(Transport transport) {
        return transport.poll(0, datagram -> {
        });
    }
}
