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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;

/**
 * Proves each invariant actually fires.
 *
 * <p>An invariant that has never failed is a comment, not a check. Every safety property here is
 * given a node that deliberately breaks it, and is asserted to notice — otherwise the whole
 * simulation could be quietly asserting nothing while thousands of seeds go green.
 */
class InvariantsTest {

    private final Simulation simulation = new Simulation(20260828L);
    private final VirtualClock clock = simulation.clock();

    @Test
    void two_leaders_in_one_term_is_caught() {
        ScriptedNode one = add(1);
        ScriptedNode two = add(2);
        simulation.checking(Invariants.atMostOneLeaderPerTerm());

        one.forceTerm(4);
        one.becomeLeader();
        simulation.step();

        two.forceTerm(4);
        two.becomeLeader();

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("node 1 and node 2 were both leader in term 4");
    }

    @Test
    void the_same_node_leading_across_two_terms_is_fine() {
        ScriptedNode one = add(1);
        simulation.checking(Invariants.atMostOneLeaderPerTerm());

        assertThatCode(() -> {
            for (long term = 1; term <= 5; term++) {
                one.forceTerm(term);
                one.becomeLeader();
                simulation.step();
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void a_term_going_backwards_is_caught() {
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, clock, store,
                java.time.Duration.ofMillis(10), n -> {
        }, null);
        simulation.add(node).checking(Invariants.termNeverDecreases());

        node.bumpTermWithoutPersisting();
        node.bumpTermWithoutPersisting();
        simulation.step();
        assertThat(node.currentTerm()).isEqualTo(2);

        simulation.crash(1);
        simulation.restart(1);

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("went from term 2 back to term 0")
                .hasMessageContaining("had not made durable");
    }

    @Test
    void a_term_that_was_persisted_survives_the_crash_that_exposes_the_other_one() {
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, clock, store,
                java.time.Duration.ofMillis(10), n -> {
        }, null);
        simulation.add(node).checking(Invariants.termNeverDecreases());

        node.bumpTerm();
        node.bumpTerm();
        simulation.step();

        simulation.crash(1);
        simulation.restart(1);

        assertThatCode(simulation::step).doesNotThrowAnyException();
        assertThat(node.currentTerm()).isEqualTo(2);
    }

    @Test
    void voting_twice_in_one_term_is_caught() {
        ScriptedNode node = add(1);
        simulation.checking(Invariants.atMostOneVotePerTerm());

        node.forceTerm(3);
        node.voteFor(2);
        simulation.step();

        node.voteFor(5);

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("voted for 2 and then for 5 in the same term 3")
                .hasMessageContaining("two candidates each win a majority");
    }

    @Test
    void a_forgotten_vote_across_a_crash_is_caught() {
        StateStore store = new MemoryStateStore();
        ScriptedNode node = new ScriptedNode(1, false, clock, store,
                java.time.Duration.ofMillis(10), n -> {
        }, null);
        simulation.add(node).checking(Invariants.atMostOneVotePerTerm());

        node.forceTerm(3);
        node.bumpTerm();
        node.voteForWithoutPersisting(2);
        simulation.step();

        simulation.crash(1);
        simulation.restart(1);
        node.forceTerm(4);
        node.voteFor(7);

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("in the same term 4");
    }

    @Test
    void voting_in_successive_terms_is_fine() {
        ScriptedNode node = add(1);
        simulation.checking(Invariants.atMostOneVotePerTerm());

        assertThatCode(() -> {
            for (int term = 1; term <= 10; term++) {
                node.forceTerm(term);
                node.voteFor(term % 3);
                simulation.step();
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void a_witness_that_leads_is_caught() {
        ScriptedNode witness = addWitness(9);
        simulation.checking(Invariants.witnessNeverLeads());

        witness.becomeLeader();

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("witness 9 reached role LEADER");
    }

    @Test
    void a_witness_that_merely_campaigns_is_caught_too() {
        ScriptedNode witness = addWitness(9);
        simulation.checking(Invariants.witnessNeverLeads());

        witness.becomeCandidate();

        assertThatThrownBy(simulation::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("reached role CANDIDATE");
    }

    @Test
    void a_witness_that_only_votes_is_fine() {
        ScriptedNode witness = addWitness(9);
        simulation.checking(Invariants.all());

        witness.forceTerm(2);
        witness.voteFor(1);

        assertThatCode(simulation::step).doesNotThrowAnyException();
    }

    @Test
    void a_failure_names_the_seed_so_it_can_be_replayed() {
        Simulation seeded = new Simulation(123456789L);
        ScriptedNode one = ScriptedNode.idle(1, seeded.clock(), new MemoryStateStore());
        ScriptedNode two = ScriptedNode.idle(2, seeded.clock(), new MemoryStateStore());
        seeded.add(one).add(two).checking(Invariants.atMostOneLeaderPerTerm());

        one.forceTerm(1);
        one.becomeLeader();
        two.forceTerm(1);
        two.becomeLeader();

        assertThatThrownBy(seeded::step)
                .isInstanceOf(SimulationFailure.class)
                .hasMessageContaining("reproduce with seed 123456789L")
                .hasMessageContaining("recent history");
    }

    @Test
    void a_healthy_cluster_satisfies_every_invariant_at_once() {
        ScriptedNode one = add(1);
        ScriptedNode two = add(2);
        ScriptedNode witness = addWitness(9);
        simulation.checking(Invariants.all());

        assertThatCode(() -> {
            for (long term = 1; term <= 20; term++) {
                one.forceTerm(term);
                two.forceTerm(term);
                witness.forceTerm(term);
                witness.voteFor(1);
                two.voteFor(1);
                one.becomeLeader();
                two.becomeFollower();
                simulation.step();
            }
        }).doesNotThrowAnyException();
    }

    private ScriptedNode add(int id) {
        ScriptedNode node = ScriptedNode.idle(id, clock, new MemoryStateStore());
        simulation.add(node);
        return node;
    }

    private ScriptedNode addWitness(int id) {
        ScriptedNode node = ScriptedNode.idleWitness(id, clock, new MemoryStateStore());
        simulation.add(node);
        return node;
    }
}
