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
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The behaviour every {@link StateStore} must have, whatever it is backed by.
 *
 * <p>Subclassed once per implementation and never copied: a conformance test that lives in two
 * places is a conformance test that will disagree with itself.
 */
abstract class StateStoreContractTest {

    private StateStore store;

    /** A store for a node that has never run. */
    protected abstract StateStore createStore();

    /** Whether this implementation claims to survive a crash. */
    protected abstract boolean expectedDurability();

    @BeforeEach
    void openStore() {
        store = createStore();
    }

    @AfterEach
    void closeStore() {
        store.close();
    }

    @Test
    void a_store_with_no_history_reads_as_a_fresh_node() {
        assertThat(store.load()).isEqualTo(PersistentState.fresh());
        assertThat(store.load().currentTerm()).isZero();
        assertThat(store.load().hasVoted()).isFalse();
    }

    @Test
    void what_was_saved_is_what_is_loaded() {
        store.save(7, 3);

        PersistentState loaded = store.load();
        assertThat(loaded.currentTerm()).isEqualTo(7);
        assertThat(loaded.votedFor()).isEqualTo(3);
        assertThat(loaded.hasVoted()).isTrue();
    }

    @Test
    void the_most_recent_save_wins() {
        store.save(1, 1);
        store.save(2, 2);
        store.save(3, PersistentState.NO_VOTE);

        assertThat(store.load()).isEqualTo(new PersistentState(3, PersistentState.NO_VOTE));
    }

    @Test
    void a_term_can_advance_without_a_vote_being_cast_in_it() {
        store.save(5, 2);
        store.save(6, PersistentState.NO_VOTE);

        assertThat(store.load().currentTerm()).isEqualTo(6);
        assertThat(store.load().hasVoted()).isFalse();
    }

    @Test
    void a_node_may_vote_for_itself() {
        store.save(9, 0);

        assertThat(store.load().votedFor()).isZero();
    }

    @Test
    void the_extremes_of_both_fields_survive() {
        store.save(Long.MAX_VALUE, 1023);

        PersistentState loaded = store.load();
        assertThat(loaded.currentTerm()).isEqualTo(Long.MAX_VALUE);
        assertThat(loaded.votedFor()).isEqualTo(1023);
    }

    @Test
    void loading_repeatedly_gives_the_same_answer() {
        store.save(4, 2);

        assertThat(store.load()).isEqualTo(store.load());
    }

    @Test
    void closing_twice_is_harmless_because_it_ends_up_in_shutdown_hooks() {
        assertThatCode(() -> {
            store.close();
            store.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void the_store_reports_whether_it_actually_survives_a_crash() {
        assertThat(store.isDurable()).isEqualTo(expectedDurability());
    }
}
