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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStateStoreTest extends StateStoreContractTest {

    @TempDir
    Path directory;

    /**
     * The contract store gets a directory to itself. The exclusive lock is per directory and
     * genuinely exclusive, so a case that opens its own store must not share one with it.
     */
    @Override
    protected StateStore createStore() {
        return new FileStateStore(directory.resolve("contract"));
    }

    @Override
    protected boolean expectedDurability() {
        return true;
    }

    @Test
    void state_survives_the_process_that_wrote_it() {
        try (StateStore first = new FileStateStore(caseDirectory())) {
            first.save(11, 2);
        }
        try (StateStore second = new FileStateStore(caseDirectory())) {
            assertThat(second.load()).isEqualTo(new PersistentState(11, 2));
        }
    }

    @Test
    void a_missing_file_is_a_node_that_has_never_run_not_an_error() throws IOException {
        try (StateStore store = new FileStateStore(directory.resolve("never-used"))) {
            assertThat(store.load()).isEqualTo(PersistentState.fresh());
        }
        assertThat(Files.isDirectory(directory.resolve("never-used"))).isTrue();
    }

    @Test
    void a_corrupt_file_refuses_to_open_rather_than_reading_as_fresh() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(9, 1);
        }
        corruptOneByte(statePath(), 9);

        try (StateStore store = new FileStateStore(caseDirectory())) {
            assertThatThrownBy(store::load)
                    .isInstanceOf(StateStoreException.class)
                    .hasMessageContaining("corrupt")
                    .hasMessageContaining("checksum mismatch")
                    .hasMessageContaining("vote twice in one term");
        }
    }

    @Test
    void corrupting_the_checksum_itself_is_caught_too() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(9, 1);
        }
        corruptOneByte(statePath(), FileStateStore.RECORD_BYTES - 1);

        try (StateStore store = new FileStateStore(caseDirectory())) {
            assertThatThrownBy(store::load).isInstanceOf(StateStoreException.class);
        }
    }

    @Test
    void a_truncated_file_refuses() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(9, 1);
        }
        byte[] full = Files.readAllBytes(statePath());
        Files.write(statePath(), java.util.Arrays.copyOf(full, full.length - 3));

        try (StateStore store = new FileStateStore(caseDirectory())) {
            assertThatThrownBy(store::load)
                    .isInstanceOf(StateStoreException.class)
                    .hasMessageContaining("expected 24 bytes but found 21");
        }
    }

    @Test
    void a_file_that_is_not_ours_at_all_refuses() throws IOException {
        Files.createDirectories(caseDirectory());
        Files.write(statePath(), new byte[FileStateStore.RECORD_BYTES]);

        try (StateStore store = new FileStateStore(caseDirectory())) {
            assertThatThrownBy(store::load)
                    .isInstanceOf(StateStoreException.class)
                    .hasMessageContaining("not a primos state file");
        }
    }

    @Test
    void a_state_file_from_a_future_version_refuses() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(9, 1);
        }
        byte[] bytes = Files.readAllBytes(statePath());
        bytes[4] = 99;
        Files.write(statePath(), bytes);

        try (StateStore store = new FileStateStore(caseDirectory())) {
            assertThatThrownBy(store::load)
                    .isInstanceOf(StateStoreException.class)
                    .hasMessageContaining("state file version 99");
        }
    }

    @Test
    void no_temporary_file_is_left_behind() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(1, 1);
            store.save(2, 2);
        }
        try (Stream<Path> files = Files.list(caseDirectory())) {
            assertThat(files.map(p -> p.getFileName().toString()))
                    .containsExactlyInAnyOrder("state", "state.lock");
        }
    }

    @Test
    void a_second_store_on_the_same_directory_refuses_to_open() {
        try (StateStore first = new FileStateStore(caseDirectory())) {
            assertThat(first.load()).isNotNull();

            assertThatThrownBy(() -> new FileStateStore(caseDirectory()))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessageContaining("another process is already using")
                    .hasMessageContaining("elects two leaders in one term");
        }
    }

    @Test
    void the_lock_is_released_on_close_so_a_restart_can_reopen() {
        new FileStateStore(directory).close();

        try (StateStore reopened = new FileStateStore(caseDirectory())) {
            assertThat(reopened.load()).isEqualTo(PersistentState.fresh());
        }
    }

    @Test
    void using_a_closed_store_throws_rather_than_silently_doing_nothing() {
        StateStore store = new FileStateStore(caseDirectory());
        store.close();

        assertThatThrownBy(() -> store.save(1, 1))
                .isInstanceOf(StateStoreException.class)
                .hasMessageContaining("is closed");
        assertThatThrownBy(store::load).isInstanceOf(StateStoreException.class);
    }

    @Test
    void the_record_is_the_size_the_format_says() throws IOException {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            store.save(1, 1);
        }

        assertThat(Files.size(statePath())).isEqualTo(FileStateStore.RECORD_BYTES);
    }

    @Test
    void every_saved_value_reloads_exactly_across_many_writes() {
        try (StateStore store = new FileStateStore(caseDirectory())) {
            for (long term = 0; term < 200; term++) {
                int vote = (term % 3 == 0) ? PersistentState.NO_VOTE : (int) (term % 1024);
                store.save(term, vote);
                assertThat(store.load()).isEqualTo(new PersistentState(term, vote));
            }
        }
    }

    private Path caseDirectory() {
        return directory.resolve("case");
    }

    private Path statePath() {
        return caseDirectory().resolve("state");
    }

    private static void corruptOneByte(Path path, int index) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        bytes[index] ^= 0x01;
        Files.write(path, bytes);
    }
}
