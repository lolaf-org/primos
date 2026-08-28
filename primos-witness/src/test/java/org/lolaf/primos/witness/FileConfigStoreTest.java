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
package org.lolaf.primos.witness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.time.VirtualClock;

class FileConfigStoreTest {

    @TempDir
    Path directory;

    private final VirtualClock clock = new VirtualClock();

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
    }

    @Test
    void an_empty_directory_serves_nobody_rather_than_failing() {
        try (FileConfigStore store = open()) {
            assertThat(store.current().applications()).isEmpty();
            assertThat(store.current().settings().registrationMode())
                    .isEqualTo(RegistrationMode.STRICT);
        }
    }

    @Test
    void the_application_name_comes_from_the_filename() throws IOException {
        application("orders", "secret=s3cret");
        application("billing", "secret=other");

        try (FileConfigStore store = open()) {
            assertThat(store.current().applicationNames()).containsExactly("billing", "orders");
            assertThat(store.current().application("orders").hasSecret()).isTrue();
        }
    }

    @Test
    void settings_are_read_from_the_parent_file() throws IOException {
        settings("bind.port=9999\nregistration.mode=permissive\nregistration.limit=7");

        try (FileConfigStore store = open()) {
            WitnessSettings settings = store.current().settings();
            assertThat(settings.bindAddress().getPort()).isEqualTo(9999);
            assertThat(settings.registrationMode()).isEqualTo(RegistrationMode.PERMISSIVE);
            assertThat(settings.autoRegistrationLimit()).isEqualTo(7);
        }
    }

    @Test
    void an_application_without_a_secret_falls_back_to_the_witness_wide_one() throws IOException {
        settings("secret=shared-by-everything");
        application("orders", "");
        application("billing", "secret=its-own");

        try (FileConfigStore store = open()) {
            assertThat(new String(store.current().application("orders").secret(),
                    StandardCharsets.UTF_8)).isEqualTo("shared-by-everything");
            assertThat(new String(store.current().application("billing").secret(),
                    StandardCharsets.UTF_8)).isEqualTo("its-own");
        }
    }

    @Test
    void a_previous_secret_is_kept_so_rotation_needs_no_flag_day() throws IOException {
        application("orders", "secret=new-one\nsecret.previous=old-one");

        try (FileConfigStore store = open()) {
            ApplicationConfig orders = store.current().application("orders");
            assertThat(new String(orders.secret(), StandardCharsets.UTF_8)).isEqualTo("new-one");
            assertThat(new String(orders.previousSecret(), StandardCharsets.UTF_8))
                    .isEqualTo("old-one");
        }
    }

    @Test
    void a_secret_can_be_kept_out_of_the_config_file() throws IOException {
        Path secretFile = directory.resolve("orders.key");
        Files.write(secretFile, "from-a-file".getBytes(StandardCharsets.UTF_8));
        application("orders", "secret=file:" + secretFile);

        try (FileConfigStore store = open()) {
            assertThat(new String(store.current().application("orders").secret(),
                    StandardCharsets.UTF_8)).isEqualTo("from-a-file");
        }
    }

    @Test
    void a_name_too_long_for_the_wire_is_refused_at_load() throws IOException {
        String tooLong = new String(new char[33]).replace('\0', 'a');
        application(tooLong, "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the wire field holds 32");
    }

    @Test
    void a_name_that_could_not_travel_on_the_wire_is_refused() throws IOException {
        application("has space", "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("printable ASCII");
    }

    // ------------------------------------------------------------ hot reload

    @Test
    void a_new_application_is_picked_up_without_a_restart() throws IOException {
        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);
            assertThat(store.current().applications()).isEmpty();

            application("orders", "secret=s3cret");
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().applicationNames()).containsExactly("orders");
            assertThat(changes.added).containsExactly("orders");
        }
    }

    @Test
    void a_removed_application_stops_being_served_but_keeps_its_state() throws IOException {
        application("orders", "secret=s3cret");
        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);

            Files.delete(applicationFile("orders"));
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().applications()).isEmpty();
            assertThat(changes.removed).containsExactly("orders");
        }
    }

    @Test
    void a_rotated_secret_is_reported_as_a_change() throws IOException {
        application("orders", "secret=old");
        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);

            application("orders", "secret=new\nsecret.previous=old");
            assertThat(rescan(store)).isTrue();

            assertThat(changes.changed).containsExactly("orders");
            assertThat(new String(store.current().application("orders").secret(),
                    StandardCharsets.UTF_8)).isEqualTo("new");
        }
    }

    @Test
    void a_malformed_file_never_takes_down_the_running_configuration() throws IOException {
        application("orders", "secret=s3cret");
        try (FileConfigStore store = open()) {
            assertThat(store.current().applicationNames()).containsExactly("orders");

            // Someone drops in a file that cannot be a valid application at three in the morning.
            application("this one has spaces", "secret=x");
            rescan(store);

            assertThat(store.current().applicationNames())
                    .as("the witness goes on serving what it was serving")
                    .containsExactly("orders");
        }
    }

    @Test
    void the_configuration_recovers_once_the_bad_file_is_fixed() throws IOException {
        application("orders", "secret=s3cret");
        try (FileConfigStore store = open()) {
            application("bad name", "secret=x");
            rescan(store);
            assertThat(store.current().applicationNames()).containsExactly("orders");

            Files.delete(applicationFile("bad name"));
            application("billing", "secret=y");
            rescan(store);

            assertThat(store.current().applicationNames()).containsExactly("billing", "orders");
        }
    }

    @Test
    void an_unchanged_directory_reports_no_change() throws IOException {
        application("orders", "secret=s3cret");
        try (FileConfigStore store = open()) {
            assertThat(rescan(store)).isFalse();
            assertThat(rescan(store)).isFalse();
        }
    }

    @Test
    void the_periodic_rescan_is_a_safety_net_for_filesystems_that_do_not_notify()
            throws IOException {
        settings("reload.rescan.seconds=30");
        try (FileConfigStore store = open()) {
            application("orders", "secret=s3cret");

            assertThat(store.poll(clock.nanoTime()))
                    .as("nothing is due yet")
                    .isFalse();

            clock.advance(Duration.ofSeconds(31));
            assertThat(store.poll(clock.nanoTime()))
                    .as("the rescan finds it even where WatchService never fires")
                    .isTrue();
            assertThat(store.current().applicationNames()).containsExactly("orders");
        }
    }

    @Test
    void the_next_check_is_never_further_away_than_the_rescan_interval() throws IOException {
        try (FileConfigStore store = open()) {
            long next = store.nextCheckNanos(clock.nanoTime());

            assertThat(next).isLessThanOrEqualTo(
                    clock.nanoTime() + Duration.ofSeconds(30).toNanos());
        }
    }

    // --------------------------------------------------------------- helpers

    private FileConfigStore open() {
        return new FileConfigStore(directory, clock);
    }

    /** Forces a reload the way the periodic safety net would, without waiting on the filesystem. */
    private boolean rescan(FileConfigStore store) {
        clock.advance(Duration.ofSeconds(31));
        return store.poll(clock.nanoTime());
    }

    private void settings(String contents) throws IOException {
        Files.write(directory.resolve("witness.properties"),
                contents.getBytes(StandardCharsets.UTF_8));
    }

    private void application(String name, String contents) throws IOException {
        Files.write(applicationFile(name), contents.getBytes(StandardCharsets.UTF_8));
    }

    private Path applicationFile(String name) {
        return directory.resolve("applications").resolve(name + ".properties");
    }

    private static final class Recording implements ConfigChangeListener {

        private final List<String> added = new ArrayList<>();
        private final List<String> changed = new ArrayList<>();
        private final List<String> removed = new ArrayList<>();

        @Override
        public void onApplicationAdded(ApplicationConfig application) {
            added.add(application.name());
        }

        @Override
        public void onApplicationChanged(ApplicationConfig previous, ApplicationConfig current) {
            changed.add(current.name());
        }

        @Override
        public void onApplicationRemoved(ApplicationConfig application) {
            removed.add(application.name());
        }
    }
}
