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

class ClusterConfigTest {

    private static ClusterConfig.Builder valid() {
        return ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(1)
                .peers(Peer.application(2, "host-b", 7000));
    }

    @Test
    void a_minimal_config_defaults_its_node_name_and_timings() {
        ClusterConfig config = valid().build();

        assertThat(config.nodeName()).isEqualTo("node-1");
        assertThat(config.timings()).isEqualTo(Timings.lan());
        assertThat(config.witness()).isEmpty();
        assertThat(config.hasWitness()).isFalse();
    }

    @Test
    void a_single_node_cluster_is_allowed() {
        ClusterConfig config = ClusterConfig.builder()
                .applicationName("dev")
                .nodeId(1)
                .build();

        assertThat(config.applicationNodeCount()).isEqualTo(1);
        assertThat(config.voterCount()).isEqualTo(1);
    }

    @Test
    void the_witness_counts_as_a_voter_but_not_as_an_application_node() {
        ClusterConfig config = valid().witness(Peer.witness(9, "referee", 7000)).build();

        assertThat(config.applicationNodeCount()).isEqualTo(2);
        assertThat(config.voterCount()).isEqualTo(3);
        assertThat(config.allPeers()).hasSize(2);
        assertThat(config.witness()).isPresent();
    }

    @Test
    void the_application_name_is_bounded_by_the_field_it_travels_in() {
        String tooLong = new String(new char[33]).replace('\0', 'a');

        assertThatThrownBy(() -> valid().applicationName(tooLong).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 32 bytes");
    }

    @Test
    void the_application_name_must_be_printable_ascii() {
        assertThatThrownBy(() -> valid().applicationName("ordres café").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("printable ASCII");
        assertThatThrownBy(() -> valid().applicationName("   ").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void duplicate_ids_are_rejected_including_this_node_appearing_among_its_own_peers() {
        assertThatThrownBy(() -> valid()
                .peers(Peer.application(2, "host-b", 7000), Peer.application(2, "host-c", 7000))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate node id 2");

        assertThatThrownBy(() -> valid()
                .peers(Peer.application(1, "host-a", 7000))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate node id 1");
    }

    @Test
    void the_witness_may_not_share_an_id_with_an_application_node() {
        assertThatThrownBy(() -> valid().witness(Peer.witness(2, "referee", 7000)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate node id 2");
    }

    @Test
    void a_witness_may_not_be_configured_as_an_ordinary_peer() {
        assertThatThrownBy(() -> valid()
                .peers(Peer.witness(9, "referee", 7000))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configure a witness with witness(...)");
    }

    @Test
    void an_application_peer_may_not_be_configured_as_the_witness() {
        assertThatThrownBy(() -> valid().witness(Peer.application(9, "referee", 7000)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Peer.witness(...)");
    }

    @Test
    void the_node_id_is_bounded_like_any_other() {
        assertThatThrownBy(() -> valid().nodeId(-5).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nodeId must be between 0 and 1023");
    }

    @Test
    void the_peer_list_is_immutable_once_built() {
        ClusterConfig config = valid().build();

        assertThatThrownBy(() -> config.peers().add(Peer.application(3, "host-c", 7000)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
