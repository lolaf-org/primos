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

import java.time.Duration;

import org.junit.jupiter.api.Test;

class TimingsTest {

    @Test
    void the_lan_profile_is_the_documented_default() {
        Timings timings = Timings.lan();

        assertThat(timings.heartbeatInterval()).isEqualTo(Duration.ofMillis(50));
        assertThat(timings.electionTimeoutMin()).isEqualTo(Duration.ofMillis(300));
        assertThat(timings.electionTimeoutMax()).isEqualTo(Duration.ofMillis(600));
        assertThat(timings.probeInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(timings.failureThreshold()).isEqualTo(3);
    }

    @Test
    void the_wan_profile_leaves_room_for_a_witness_on_another_network() {
        Timings timings = Timings.wan();

        assertThat(timings.electionTimeoutMin()).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(timings.electionTimeoutMin())
                .isGreaterThanOrEqualTo(timings.heartbeatInterval().multipliedBy(5));
    }

    @Test
    void the_election_timeout_must_span_several_heartbeats() {
        assertThatThrownBy(() -> Timings.builder()
                .heartbeatInterval(Duration.ofMillis(100))
                .electionTimeout(Duration.ofMillis(200), Duration.ofMillis(400))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 5 heartbeat intervals");
    }

    /**
     * Reachable only because the two bounds have setters of their own: raising the low one past the
     * high one's default inverts the range, and the message has to name that cause rather than
     * point at the bound the caller never touched.
     */
    @Test
    void raising_one_election_bound_past_the_others_default_is_refused() {
        assertThatThrownBy(() -> Timings.builder()
                .electionTimeoutMin(Duration.ofSeconds(5))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("electionTimeoutMin (5000ms) is above electionTimeoutMax")
                .hasMessageContaining("set both");
    }

    @Test
    void the_two_bounds_can_be_set_one_at_a_time_as_long_as_the_result_holds() {
        Timings timings = Timings.builder()
                .electionTimeoutMax(Duration.ofSeconds(4))
                .electionTimeoutMin(Duration.ofSeconds(2))
                .build();

        assertThat(timings.electionTimeoutMin()).isEqualTo(Duration.ofSeconds(2));
        assertThat(timings.electionTimeoutMax()).isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    void the_randomisation_range_must_not_be_empty() {
        assertThatThrownBy(() -> Timings.builder()
                .electionTimeout(Duration.ofMillis(300), Duration.ofMillis(300))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than electionTimeoutMin");
    }

    @Test
    void the_randomisation_range_must_be_wide_enough_to_break_a_tie() {
        assertThatThrownBy(() -> Timings.builder()
                .heartbeatInterval(Duration.ofMillis(50))
                .electionTimeout(Duration.ofMillis(300), Duration.ofMillis(310))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("split the vote");
    }

    @Test
    void the_advertisement_interval_defaults_to_thirty_seconds() {
        assertThat(Timings.lan().advertisementInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(Timings.wan().advertisementInterval()).isEqualTo(Duration.ofSeconds(30));
    }

    /**
     * The values a node advertises cannot change while it runs, so restating them more often than
     * it checks whether its peers are alive at all is pure traffic.
     */
    @Test
    void advertising_more_often_than_probing_is_rejected() {
        assertThatThrownBy(() -> Timings.builder()
                .probeInterval(Duration.ofSeconds(2))
                .advertisementInterval(Duration.ofSeconds(1))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be shorter than probeInterval");
    }

    @Test
    void probing_less_often_than_the_nat_ceiling_is_rejected() {
        assertThatThrownBy(() -> Timings.builder()
                .probeInterval(Duration.ofSeconds(30))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NAT pinhole");
    }

    @Test
    void zero_and_negative_intervals_are_rejected() {
        assertThatThrownBy(() -> Timings.builder().heartbeatInterval(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeatInterval must be positive");
        assertThatThrownBy(() -> Timings.builder().failureThreshold(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureThreshold must be at least 1");
    }
}
