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

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;

class LeadershipConfirmationTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);
    private static final Timings TIMINGS = Timings.lan();

    @Test
    void a_leader_is_confirmed_by_its_quorum_within_a_heartbeat_or_so() {
        Simulation simulation = simulation(1L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.run(Duration.ofSeconds(2));

        long now = simulation.clock().nanoTime();
        long confirmed = cluster.leader().get().election().leadershipConfirmedNanos();

        assertThat(confirmed).isLessThanOrEqualTo(now);
        assertThat(now - confirmed).isLessThan(TIMINGS.heartbeatInterval().multipliedBy(3).toNanos());
    }

    @Test
    void a_follower_is_never_confirmed() {
        Simulation simulation = simulation(2L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            if (node.role() != Role.LEADER) {
                assertThat(node.election().leadershipConfirmedNanos()).isEqualTo(Long.MIN_VALUE);
            }
        }
    }

    @Test
    void a_single_node_is_confirmed_by_itself_at_every_instant() {
        Simulation simulation = simulation(3L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 1, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);

        assertThat(cluster.leader().get().election().leadershipConfirmedNanos()).isEqualTo(simulation.clock().nanoTime());
    }

    @Test
    void a_cut_off_leader_stops_being_confirmed_well_before_anyone_replaces_it() {
        for (long seed = 1; seed <= 20; seed++) {
            Simulation simulation = simulation(seed);
            ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
            simulation.runUntil(cluster::hasSingleLeader, SETTLE);
            ElectionCluster.NodeUnderTest old = cluster.leader().get();
            long[] lastConfirmed = {Long.MIN_VALUE};
            simulation.checking(new Invariant() {
                @Override
                public String name() {
                    return "records the old leader's latest confirmation";
                }

                @Override
                public void check(Simulation ignored) {
                    lastConfirmed[0] = Math.max(lastConfirmed[0], old.election().leadershipConfirmedNanos());
                }
            });

            simulation.network().isolate(old.id());
            assertThat(simulation.runUntil(() -> cluster.nodes().stream()
                    .anyMatch(n -> n != old && n.role() == Role.LEADER), SETTLE)).isTrue();
            long electedAt = simulation.clock().nanoTime();

            assertThat(electedAt - lastConfirmed[0])
                    .as("seed %s: a lease ending L after the last confirmation ends before a successor's L wait", seed)
                    .isGreaterThanOrEqualTo(TIMINGS.electionTimeoutMin().toNanos());
        }
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }
}
