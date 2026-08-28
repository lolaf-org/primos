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
package org.lolaf.primos.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PersistentStateTest {

    @Test
    void a_fresh_node_is_term_zero_with_no_vote() {
        assertThat(PersistentState.fresh().currentTerm()).isZero();
        assertThat(PersistentState.fresh().votedFor()).isEqualTo(PersistentState.NO_VOTE);
        assertThat(PersistentState.fresh().hasVoted()).isFalse();
    }

    @Test
    void a_negative_term_is_impossible() {
        assertThatThrownBy(() -> new PersistentState(-1, PersistentState.NO_VOTE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currentTerm must not be negative");
    }

    @Test
    void a_vote_must_be_for_a_plausible_node_id() {
        assertThatThrownBy(() -> new PersistentState(1, 5000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("votedFor must be between 0 and 1023");
    }

    @Test
    void the_no_vote_sentinel_is_not_treated_as_an_id() {
        assertThat(new PersistentState(3, PersistentState.NO_VOTE).hasVoted()).isFalse();
    }

    @Test
    void states_with_the_same_values_are_equal() {
        assertThat(new PersistentState(3, 1)).isEqualTo(new PersistentState(3, 1));
        assertThat(new PersistentState(3, 1)).isNotEqualTo(new PersistentState(3, 2));
    }
}
