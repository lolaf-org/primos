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

class PeerTest {

    @Test
    void an_application_peer_defaults_its_name_from_its_id() {
        Peer peer = Peer.application(3, "host-a", 7000);

        assertThat(peer.id()).isEqualTo(3);
        assertThat(peer.name()).isEqualTo("node-3");
        assertThat(peer.type()).isEqualTo(PeerType.APPLICATION);
        assertThat(peer.isWitness()).isFalse();
    }

    @Test
    void a_witness_is_named_and_typed_differently() {
        Peer peer = Peer.witness(9, "referee", 7000);

        assertThat(peer.name()).isEqualTo("witness-9");
        assertThat(peer.type()).isEqualTo(PeerType.WITNESS);
        assertThat(peer.isWitness()).isTrue();
    }

    @Test
    void a_given_name_wins_over_the_default() {
        assertThat(Peer.application(1, "orders-a", "host", 7000).name()).isEqualTo("orders-a");
    }

    @Test
    void a_hostname_is_kept_as_written_and_never_resolved_here() {
        // A name that cannot resolve still builds, which is the property that matters: DNS can
        // block for seconds and must not run wherever a Peer happens to be constructed. The
        // transport resolves it once at startup, and refuses to start if it cannot.
        Peer peer = Peer.application(1, "no-such-host.invalid", 7000);

        assertThat(peer.host()).isEqualTo("no-such-host.invalid");
        assertThat(peer.port()).isEqualTo(7000);
    }

    @Test
    void ids_are_bounded_so_a_mistyped_port_fails_loudly_rather_than_becoming_a_peer() {
        assertThatThrownBy(() -> Peer.application(-1, "host", 7000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("peer id must be between 0 and 1023");
        assertThatThrownBy(() -> Peer.application(7000, "host", 7000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("7000");
    }

    @Test
    void ids_may_be_sparse_so_operators_can_use_a_scheme_that_means_something() {
        assertThat(Peer.application(101, "dc1-a", 7000).id()).isEqualTo(101);
        assertThat(Peer.application(201, "dc2-a", 7000).id()).isEqualTo(201);
        assertThat(Peer.application(Peer.MAX_ID, "host", 7000).id()).isEqualTo(1023);
    }

    @Test
    void a_blank_host_or_out_of_range_port_is_rejected() {
        assertThatThrownBy(() -> Peer.application(1, "  ", 7000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be blank");
        assertThatThrownBy(() -> Peer.application(1, "host", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");
    }

    @Test
    void peers_with_the_same_values_are_equal() {
        assertThat(Peer.application(1, "host", 7000)).isEqualTo(Peer.application(1, "host", 7000));
        assertThat(Peer.application(1, "host", 7000)).isNotEqualTo(Peer.witness(1, "host", 7000));
    }
}
