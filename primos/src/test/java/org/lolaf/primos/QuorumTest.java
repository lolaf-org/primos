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
package org.lolaf.primos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class QuorumTest {

    private static Quorum quorum(int applicationNodes, boolean witness) {
        List<Peer> peers = new ArrayList<>();
        for (int id = 2; id <= applicationNodes; id++) {
            peers.add(Peer.application(id, "host-" + id, 7000));
        }
        ClusterConfig.Builder builder = ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(1)
                .peers(peers);
        if (witness) {
            builder.witness(Peer.witness(99, "referee", 7000));
        }
        return Quorum.of(builder.build());
    }

    @Test
    void two_nodes_plus_a_witness_is_the_configuration_this_library_exists_for() {
        Quorum q = quorum(2, true);

        assertThat(q.voters()).isEqualTo(3);
        assertThat(q.required()).isEqualTo(2);
        assertThat(q.faultTolerance()).isEqualTo(1);
        assertThat(q.warnings()).isEmpty();
        assertThat(q.summary())
                .isEqualTo("cluster 'orders': 3 voters (2 nodes + witness), quorum 2, tolerates 1 failure");
    }

    @Test
    void two_nodes_alone_tolerate_nothing_and_say_so() {
        Quorum q = quorum(2, false);

        assertThat(q.faultTolerance()).isZero();
        assertThat(q.warnings()).singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("tolerates zero failures")
                .contains("third failure domain");
    }

    @Test
    void a_witness_on_an_odd_cluster_is_warned_about_rather_than_refused() {
        Quorum q = quorum(3, true);

        assertThat(q.voters()).isEqualTo(4);
        assertThat(q.required()).isEqualTo(3);
        assertThat(q.faultTolerance()).isEqualTo(1);
        assertThat(q.warnings()).singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("the witness does not help here")
                .contains("Consider removing it");
    }

    @Test
    void three_nodes_alone_are_a_healthy_shape() {
        Quorum q = quorum(3, false);

        assertThat(q.voters()).isEqualTo(3);
        assertThat(q.required()).isEqualTo(2);
        assertThat(q.faultTolerance()).isEqualTo(1);
        assertThat(q.warnings()).isEmpty();
    }

    @Test
    void an_even_cluster_without_a_witness_is_told_what_one_would_buy() {
        Quorum q = quorum(4, false);

        assertThat(q.required()).isEqualTo(3);
        assertThat(q.faultTolerance()).isEqualTo(1);
        assertThat(q.warnings()).singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("Adding a witness")
                .contains("tolerating 2 failures");
    }

    @Test
    void four_nodes_plus_a_witness_tolerate_two_failures() {
        Quorum q = quorum(4, true);

        assertThat(q.voters()).isEqualTo(5);
        assertThat(q.required()).isEqualTo(3);
        assertThat(q.faultTolerance()).isEqualTo(2);
        assertThat(q.warnings()).isEmpty();
    }

    @Test
    void a_single_node_is_permanently_leader_and_says_it_tolerates_nothing() {
        Quorum q = quorum(1, false);

        assertThat(q.voters()).isEqualTo(1);
        assertThat(q.required()).isEqualTo(1);
        assertThat(q.faultTolerance()).isZero();
        assertThat(q.summary())
                .isEqualTo("cluster 'orders': 1 voters (1 node), quorum 1, tolerates 0 failures");
        assertThat(q.warnings()).isEmpty();
    }

    @Test
    void a_witness_beside_a_single_node_makes_things_worse_and_is_warned_about() {
        Quorum q = quorum(1, true);

        assertThat(q.voters()).isEqualTo(2);
        assertThat(q.required()).isEqualTo(2);
        assertThat(q.faultTolerance()).isZero();
        assertThat(q.warnings()).isNotEmpty();
    }

    @Test
    void a_cluster_bigger_than_raft_benefits_from_is_warned_about_not_refused() {
        Quorum q = quorum(9, false);

        assertThat(q.voters()).isEqualTo(9);
        assertThat(q.required()).isEqualTo(5);
        assertThat(q.warnings())
                .anySatisfy(w -> assertThat(w).contains("larger than Raft clusters benefit from"));
    }

    @Test
    void seven_voters_is_the_last_size_that_passes_without_comment() {
        assertThat(quorum(7, false).warnings()).isEmpty();
        assertThat(quorum(6, true).warnings()).isEmpty();
        assertThat(quorum(8, false).warnings()).isNotEmpty();
    }

    @Test
    void the_size_warning_is_independent_of_the_shape_warning() {
        Quorum q = quorum(9, true);

        assertThat(q.warnings()).hasSize(2);
    }

    @Test
    void a_witness_and_an_application_node_compute_the_same_quorum_for_one_cluster() {
        Quorum fromApplicationNode = quorum(2, true);
        Quorum fromWitness = Quorum.of(ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(99)
                .asWitness()
                .peers(Peer.application(1, "host-1", 7000), Peer.application(2, "host-2", 7000))
                .build());

        assertThat(fromWitness.voters()).isEqualTo(fromApplicationNode.voters());
        assertThat(fromWitness.required()).isEqualTo(fromApplicationNode.required());
        assertThat(fromWitness.summary()).isEqualTo(fromApplicationNode.summary());
    }

    @Test
    void quorum_is_a_majority_of_configured_voters_not_of_reachable_ones() {
        Quorum q = quorum(5, false);

        assertThat(q.isReached(2)).isFalse();
        assertThat(q.isReached(3)).isTrue();
        assertThat(q.isReached(5)).isTrue();
    }
}
