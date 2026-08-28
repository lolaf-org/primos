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

import org.junit.jupiter.api.Test;

class MemoryStateStoreTest extends StateStoreContractTest {

    @Override
    protected StateStore createStore() {
        return new MemoryStateStore();
    }

    @Override
    protected boolean expectedDurability() {
        return false;
    }

    @Test
    void it_admits_it_is_not_durable_which_is_what_the_witness_refuses_on() {
        try (StateStore store = new MemoryStateStore()) {
            assertThat(store.isDurable())
                    .as("a stateless witness is the double-vote bug, so the server refuses this")
                    .isFalse();
        }
    }

    @Test
    void it_can_start_partway_through_a_cluster_s_life_for_tests() {
        try (StateStore store = new MemoryStateStore(new PersistentState(12, 4))) {
            assertThat(store.load().currentTerm()).isEqualTo(12);
            assertThat(store.load().votedFor()).isEqualTo(4);
        }
    }
}
