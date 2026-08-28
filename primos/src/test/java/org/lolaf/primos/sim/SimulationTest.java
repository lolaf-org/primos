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
package org.lolaf.primos.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.Transport;

class SimulationTest {

    @Test
    void the_same_seed_produces_a_byte_identical_run() {
        assertThat(chatteringCluster(20260828L)).isEqualTo(chatteringCluster(20260828L));
    }

    @Test
    void a_different_seed_produces_a_different_run() {
        assertThat(chatteringCluster(1L)).isNotEqualTo(chatteringCluster(2L));
    }

    @Test
    void an_hour_of_cluster_life_costs_no_wall_clock_time() {
        Simulation simulation = new Simulation(7L);
        simulation.add(ScriptedNode.idle(1, simulation.clock(), new MemoryStateStore()));

        long startedAt = System.nanoTime();
        simulation.run(Duration.ofHours(1));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        assertThat(simulation.clock().nanoTime()).isEqualTo(Duration.ofHours(1).toNanos());
        assertThat(elapsedMillis).isLessThan(5_000);
    }

    @Test
    void time_jumps_to_the_next_event_rather_than_ticking_through_the_gaps() {
        Simulation simulation = new Simulation(7L);
        VirtualClock clock = simulation.clock();
        simulation.add(new ScriptedNode(1, false, clock, new MemoryStateStore(),
                Duration.ofSeconds(30), node -> {
        }, null));

        simulation.step();

        assertThat(clock.nanoTime()).isEqualTo(Duration.ofSeconds(30).toNanos());
        assertThat(simulation.steps()).isEqualTo(1);
    }

    @Test
    void a_crashed_node_is_not_ticked_and_a_restarted_one_is() {
        Simulation simulation = new Simulation(7L);
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, simulation.clock(), store,
                Duration.ofMillis(10), ScriptedNode::bumpTerm, null);
        simulation.add(node);

        simulation.run(Duration.ofMillis(50));
        long termBeforeCrash = node.currentTerm();
        assertThat(termBeforeCrash).isPositive();

        simulation.crash(1);
        simulation.run(Duration.ofMillis(100));
        assertThat(node.isRunning()).isFalse();

        simulation.restart(1);
        simulation.run(Duration.ofMillis(50));

        assertThat(node.isRunning()).isTrue();
        assertThat(node.currentTerm()).isGreaterThan(termBeforeCrash);
    }

    @Test
    void a_restarted_node_reads_back_exactly_what_it_persisted() {
        Simulation simulation = new Simulation(7L);
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, simulation.clock(), store,
                Duration.ofMillis(10), n -> {
        }, null);
        simulation.add(node);

        node.forceTerm(6);
        node.voteFor(3);

        simulation.crash(1);
        simulation.restart(1);

        assertThat(node.currentTerm()).isEqualTo(6);
        assertThat(node.votedFor()).isEqualTo(3);
        assertThat(node.role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    void a_crash_discards_exactly_what_was_never_persisted() {
        Simulation simulation = new Simulation(7L);
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, simulation.clock(), store,
                Duration.ofMillis(10), n -> {
        }, null);
        simulation.add(node);

        node.bumpTerm();
        node.voteForWithoutPersisting(4);

        simulation.crash(1);
        simulation.restart(1);

        assertThat(node.currentTerm()).isEqualTo(1);
        assertThat(node.votedFor())
                .as("the vote was never made durable, so the crash must lose it")
                .isEqualTo(PersistentState.NO_VOTE);
    }

    @Test
    void run_until_stops_as_soon_as_the_condition_holds() {
        Simulation simulation = new Simulation(7L);
        ScriptedNode node = new ScriptedNode(1, false, simulation.clock(), new MemoryStateStore(),
                Duration.ofMillis(10), ScriptedNode::bumpTerm, null);
        simulation.add(node);

        boolean reached = simulation.runUntil(() -> node.currentTerm() >= 5, Duration.ofMinutes(1));

        assertThat(reached).isTrue();
        assertThat(node.currentTerm()).isEqualTo(5);
        assertThat(simulation.clock().nanoTime()).isLessThan(Duration.ofSeconds(1).toNanos());
    }

    @Test
    void run_until_gives_up_at_its_limit() {
        Simulation simulation = new Simulation(7L);
        ScriptedNode node = ScriptedNode.idle(1, simulation.clock(), new MemoryStateStore());
        simulation.add(node);

        boolean reached = simulation.runUntil(() -> node.currentTerm() > 0, Duration.ofSeconds(1));

        assertThat(reached).isFalse();
    }

    @Test
    void nodes_are_ticked_in_id_order_regardless_of_the_order_they_were_added() {
        Simulation simulation = new Simulation(7L);
        simulation.add(ScriptedNode.idle(3, simulation.clock(), new MemoryStateStore()));
        simulation.add(ScriptedNode.idle(1, simulation.clock(), new MemoryStateStore()));
        simulation.add(ScriptedNode.idle(2, simulation.clock(), new MemoryStateStore()));

        assertThat(simulation.nodes()).extracting(SimNode::id).containsExactly(1, 2, 3);
    }

    @Test
    void the_digest_records_a_crash_and_a_restart() {
        Simulation simulation = new Simulation(7L);
        simulation.add(ScriptedNode.idle(1, simulation.clock(), new MemoryStateStore()));

        simulation.step();
        simulation.crash(1);
        simulation.restart(1);

        assertThat(simulation.traceDigest()).contains("crash n1").contains("restart n1");
        assertThat(simulation.recentTrace()).contains("restart n1");
    }

    @Test
    void a_partitioned_cluster_stops_exchanging_messages() {
        Simulation simulation = new Simulation(11L);
        VirtualClock clock = simulation.clock();
        Transport one = simulation.network().transportFor(1);
        Transport two = simulation.network().transportFor(2);

        ScriptedNode a = new ScriptedNode(1, false, clock, new MemoryStateStore(),
                Duration.ofMillis(10), n -> {
        }, one, 2);
        ScriptedNode b = new ScriptedNode(2, false, clock, new MemoryStateStore(),
                Duration.ofMillis(10), n -> {
        }, two, 1);
        simulation.add(a).add(b);

        simulation.run(Duration.ofMillis(100));
        assertThat(b.datagramsReceived()).isPositive();

        int beforePartition = b.datagramsReceived();
        simulation.network().partition(new int[] {1}, new int[] {2});
        simulation.run(Duration.ofMillis(200));

        assertThat(b.datagramsReceived()).isEqualTo(beforePartition);
        assertThat(simulation.network().droppedByPartition()).isPositive();
    }

    /** A cluster that talks over a lossy, reordering network, to give the digest something to record. */
    private static String chatteringCluster(long seed) {
        Simulation simulation = new Simulation(seed);
        VirtualClock clock = simulation.clock();
        simulation.network().lossProbability(0.2);
        simulation.network().delayRange(0, TimeUnit.MILLISECONDS.toNanos(30));

        for (int id = 1; id <= 3; id++) {
            int[] peers = otherThan(id);
            simulation.add(new ScriptedNode(id, false, clock, new MemoryStateStore(),
                    Duration.ofMillis(10 + id), ScriptedNode::bumpTerm,
                    simulation.network().transportFor(id), peers));
        }
        simulation.checking(Invariants.termNeverDecreases());
        simulation.run(Duration.ofSeconds(5));
        return simulation.traceDigest();
    }

    private static int[] otherThan(int id) {
        int[] peers = new int[2];
        int index = 0;
        for (int candidate = 1; candidate <= 3; candidate++) {
            if (candidate != id) {
                peers[index++] = candidate;
            }
        }
        return peers;
    }
}
