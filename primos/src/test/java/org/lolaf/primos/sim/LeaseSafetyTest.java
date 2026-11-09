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
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Timings;

/**
 * A leader's lease against randomly failing links, where a pre-vote granted before a heartbeat and a real vote
 * arriving after it are ordinary.
 */
class LeaseSafetyTest {

    private static final Timings TIMINGS = Timings.lan();

    @Test
    void no_successor_leads_during_a_lease_whatever_the_links_do() {
        int elections = 0;
        for (long seed = 1; seed <= 300; seed++) {
            elections += chaos(seed);
        }
        assertThat(elections).as("the run has to exercise failovers to prove anything").isGreaterThan(300);
    }

    private static int chaos(long seed) {
        Simulation simulation = new Simulation(seed).checking(Invariants.all());
        simulation.network().delayRange(Duration.ofMillis(1).toNanos(), Duration.ofMillis(150).toNanos());
        simulation.network().lossProbability(0.3);
        int voters = 5;
        ElectionCluster cluster = ElectionCluster.of(simulation, voters, false);
        simulation.checking(Invariants.noSuccessorDuringALease(TIMINGS.electionTimeoutMin(),
                node -> ((ElectionCluster.NodeUnderTest) node).election().leadershipConfirmedNanos()));

        Random links = new Random(seed);
        for (int round = 0; round < 80; round++) {
            int from = 1 + links.nextInt(voters);
            int to = 1 + links.nextInt(voters);
            if (from != to) {
                if (links.nextBoolean()) {
                    simulation.network().sever(from, to);
                } else {
                    simulation.network().restore(from, to);
                }
            }
            simulation.run(Duration.ofMillis(links.nextInt(150)));
        }
        long highestTerm = 0;
        for (ElectionCluster.NodeUnderTest node : cluster.nodes()) {
            highestTerm = Math.max(highestTerm, node.currentTerm());
        }
        return (int) highestTerm;
    }
}
