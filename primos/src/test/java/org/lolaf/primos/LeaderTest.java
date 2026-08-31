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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The three states a leader can be in, and the one distinction the type exists to make: an empty
 * {@link Leader#peer()} never means "nobody is leading".
 */
class LeaderTest {

    @Test
    void this_node_leading_is_present_and_says_so() {
        Leader leader = Leader.self(3);

        assertThat(leader.id()).isEqualTo(3);
        assertThat(leader.name())
                .as("derived by the same rule Peer uses, rather than configured: a node handing its"
                        + " own name back to the application that set it bought nothing")
                .isEqualTo("node-3");
        assertThat(leader.isSelf()).isTrue();
        assertThat(leader.peer())
                .as("there is nowhere to forward work to when the work is already here")
                .isEmpty();
    }

    @Test
    void a_peer_leading_carries_the_address_to_forward_to() {
        Peer peer = Peer.application(4, "orders-b", "10.0.0.4", 7000);

        Leader leader = Leader.at(peer);

        assertThat(leader.id()).isEqualTo(4);
        assertThat(leader.name())
                .as("the peer's own name, so a log line needs no lookup")
                .isEqualTo("orders-b");
        assertThat(leader.isSelf()).isFalse();
        assertThat(leader.peer()).contains(peer);
    }

    @Test
    void a_leader_known_only_by_id_has_neither() {
        Leader leader = Leader.byId(7);

        assertThat(leader.id()).isEqualTo(7);
        assertThat(leader.name()).isEqualTo("node-7");
        assertThat(leader.isSelf())
                .as("a witness server hearing an unlearned node is not the node it heard")
                .isFalse();
        assertThat(leader.peer()).isEmpty();
    }

    @Test
    void an_absent_peer_is_never_the_same_question_as_an_absent_leader() {
        // The reason this type exists rather than Optional<Peer>: both of these lead, and a bare
        // Optional<Peer> would report both as empty — the value that means nobody leads.
        assertThat(Leader.self(1).peer()).isEmpty();
        assertThat(Leader.byId(1).peer()).isEmpty();
        assertThat(Leader.self(1)).isNotEqualTo(Leader.byId(1));
    }

    @Test
    void an_id_the_wire_could_not_carry_is_refused() {
        assertThatThrownBy(() -> Leader.byId(Peer.MAX_ID + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leader id");
    }
}
