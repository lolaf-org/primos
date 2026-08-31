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
import org.lolaf.primos.Timings;
import org.lolaf.primos.wire.MessageAuthenticator;
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
    void an_application_may_name_its_own_mac_algorithm() throws IOException {
        // Per application, because a witness serves clusters it does not own: one of them moving
        // to another algorithm must not be a flag day for every other tenant.
        settings("secret=shared-by-everything");
        application("orders", "secret=its-own\nsecret.algorithm=HmacSHA512");
        application("billing", "secret=its-own");

        try (FileConfigStore store = open()) {
            assertThat(store.current().application("orders").macAlgorithm())
                    .isEqualTo("HmacSHA512");
            assertThat(store.current().application("billing").macAlgorithm())
                    .as("silence means the same default the library has")
                    .isEqualTo(MessageAuthenticator.DEFAULT_ALGORITHM);
        }
    }

    @Test
    void the_witness_wide_algorithm_covers_applications_that_do_not_name_one() throws IOException {
        settings("secret=shared-by-everything\nsecret.algorithm=HmacSHA512");
        application("orders", "");

        try (FileConfigStore store = open()) {
            assertThat(store.current().application("orders").macAlgorithm())
                    .isEqualTo("HmacSHA512");
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

    // ------------------------------------------------------------ timings

    @Test
    void nothing_configured_is_the_lan_profile() throws IOException {
        application("orders", "secret=x");

        assertThat(open().current().settings().timings()).isEqualTo(Timings.lan());
    }

    @Test
    void a_named_profile_is_taken_whole() throws IOException {
        settings("timings.profile=wan");
        application("orders", "secret=x");

        assertThat(open().current().settings().timings()).isEqualTo(Timings.wan());
    }

    /**
     * The reason the witness needs this at all: a cluster running intervals that are neither
     * {@code lan} nor {@code wan} had no witness setting that could match it, and a witness with a
     * longer {@code electionTimeoutMin} than its cluster is proportionally more reluctant to grant
     * the pre-vote that failover waits on. Nothing detects that; it is just slow.
     */
    @Test
    void the_intervals_can_be_spelled_out_so_a_witness_can_match_a_custom_cluster() throws IOException {
        settings(String.join("\n",
                "timings.heartbeat.millis=250",
                "timings.election.min.millis=2000",
                "timings.election.max.millis=4000",
                "timings.probe.millis=3000",
                "timings.failure.threshold=5"));
        application("orders", "secret=x");

        Timings timings = open().current().settings().timings();

        assertThat(timings).isEqualTo(Timings.builder()
                .heartbeatInterval(Duration.ofMillis(250))
                .electionTimeout(Duration.ofSeconds(2), Duration.ofSeconds(4))
                .probeInterval(Duration.ofSeconds(3))
                .failureThreshold(5)
                .build());
    }

    @Test
    void a_profile_and_an_explicit_interval_together_are_refused() throws IOException {
        settings(String.join("\n", "timings.profile=wan", "timings.heartbeat.millis=250"));
        application("orders", "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timings.profile is set to 'wan'")
                .hasMessageContaining("timings.heartbeat.millis")
                .hasMessageContaining("Choose one");
    }

    @Test
    void one_election_bound_without_its_pair_is_refused() throws IOException {
        settings("timings.election.min.millis=2000");
        application("orders", "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without its pair");
    }

    @Test
    void an_incoherent_custom_set_fails_with_the_librarys_own_message() throws IOException {
        settings("timings.heartbeat.millis=100");
        application("orders", "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("electionTimeoutMin must be at least");
    }

    @Test
    void a_duration_that_is_not_a_number_says_which_key() throws IOException {
        settings("timings.heartbeat.millis=250ms");
        application("orders", "secret=x");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timings.heartbeat.millis must be a whole number of"
                        + " milliseconds");
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
