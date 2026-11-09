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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Leader;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.RecordingStats;

/**
 * A follower stops naming a leader it no longer hears from, even when no election can follow.
 */
class LeaderSilenceTest {

    private static final Duration SETTLE = Duration.ofSeconds(5);

    @Test
    void a_survivor_without_a_quorum_stops_naming_the_dead_leader() {
        Simulation simulation = simulation(11L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        ElectionCluster.NodeUnderTest survivor = crashAllButOneFollower(cluster);

        simulation.run(Duration.ofSeconds(3));

        assertThat(survivor.election().leaderOptional()).isEmpty();
    }

    @Test
    void a_withdrawn_node_with_no_election_timer_stops_naming_it_too() {
        Simulation simulation = simulation(12L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int leader = cluster.leader().get().id();
        ElectionCluster.NodeUnderTest survivor = followers(cluster, leader).get(0);
        survivor.election().setCandidate(false);
        simulation.run(Duration.ofMillis(200));
        assertThat(survivor.election().leaderOptional()).isPresent();

        crashAllButOneFollower(cluster, survivor);
        simulation.run(Duration.ofSeconds(3));

        assertThat(survivor.election().leaderOptional()).isEmpty();
    }

    @Test
    void a_follower_that_keeps_hearing_its_leader_never_loses_it() {
        Simulation simulation = simulation(13L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        List<Optional<Leader>> reported = new ArrayList<>();
        cluster.nodes().forEach(node -> node.attach(new PrimosListener() {
            @Override
            public void onLeaderChanged(Optional<Leader> leader, long term) {
                reported.add(leader);
            }
        }));
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.run(Duration.ofMillis(200));
        reported.clear();

        simulation.run(Duration.ofSeconds(5));

        assertThat(reported).isEmpty();
    }

    @Test
    void the_leaderless_time_counts_from_the_silence_rather_than_the_next_election() {
        Simulation simulation = simulation(14L);
        ElectionCluster cluster = ElectionCluster.of(simulation, 3, false);
        RecordingStats stats = new RecordingStats();
        int leader = awaitLeader(simulation, cluster);
        ElectionCluster.NodeUnderTest follower = followers(cluster, leader).get(0);
        follower.attach(stats);
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        int current = cluster.leader().get().id();
        simulation.run(Duration.ofMillis(200));
        synchronized (stats.leaderlessPeriods) {
            stats.leaderlessPeriods.clear();
        }

        cluster.node(current).crash();
        int other = followers(cluster, current).stream()
                .filter(node -> node.id() != follower.id())
                .findFirst().get().id();
        simulation.network().sever(follower.id(), other);
        simulation.network().sever(other, follower.id());
        simulation.run(Duration.ofSeconds(2));
        simulation.network().heal();
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        simulation.run(Duration.ofMillis(200));

        assertThat(stats.leaderlessPeriods).isNotEmpty();
        assertThat(Duration.ofNanos(stats.leaderlessPeriods.get(0)))
                .as("two seconds passed with nobody able to win")
                .isGreaterThan(Duration.ofSeconds(1));
    }

    private static int awaitLeader(Simulation simulation, ElectionCluster cluster) {
        simulation.runUntil(cluster::hasSingleLeader, SETTLE);
        return cluster.leader().get().id();
    }

    private static List<ElectionCluster.NodeUnderTest> followers(ElectionCluster cluster, int leader) {
        return cluster.nodes().stream().filter(node -> node.id() != leader).collect(Collectors.toList());
    }

    private static ElectionCluster.NodeUnderTest crashAllButOneFollower(ElectionCluster cluster) {
        int leader = cluster.leader().get().id();
        return crashAllButOneFollower(cluster, followers(cluster, leader).get(0));
    }

    private static ElectionCluster.NodeUnderTest crashAllButOneFollower(ElectionCluster cluster,
                                                                        ElectionCluster.NodeUnderTest survivor) {
        cluster.nodes().stream().filter(node -> node.id() != survivor.id()).forEach(ElectionCluster.NodeUnderTest::crash);
        return survivor;
    }

    private static Simulation simulation(long seed) {
        return new Simulation(seed).checking(Invariants.all());
    }
}
