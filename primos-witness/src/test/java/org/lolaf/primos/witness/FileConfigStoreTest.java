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
            assertThat(store.current().settings().configSource())
                    .isEqualTo(ConfigSource.FILE);
        }
    }

    @Test
    void the_application_name_comes_from_the_filename() throws IOException {
        application("orders", "");
        application("billing", "");

        try (FileConfigStore store = open()) {
            assertThat(store.current().applicationNames()).containsExactly("billing", "orders");
        }
    }

    @Test
    void settings_are_read_from_the_parent_file() throws IOException {
        settings("bind.port=9999\nconfig.source=cluster\nconfig.cluster.limit=7");

        try (FileConfigStore store = open()) {
            WitnessSettings settings = store.current().settings();
            assertThat(settings.bindAddress().getPort()).isEqualTo(9999);
            assertThat(settings.configSource()).isEqualTo(ConfigSource.CLUSTER);
            assertThat(settings.clusterApplicationLimit()).isEqualTo(7);
        }
    }

    @Test
    void keys_are_read_from_the_witness_file_with_their_algorithms() throws IOException {
        settings(String.join("\n",
                "secret.platform.value = shared-by-everything",
                "secret.orders.value = its-own",
                "secret.orders.algorithm = HmacSHA512",
                "secret.orders.applications = orders-*, billing"));

        try (FileConfigStore store = open()) {
            Keyring keyring = store.current().settings().keyring();

            assertThat(keyring.keys()).extracting(WitnessKey::name)
                    .as("sorted, so a ring does not depend on Properties hashing")
                    .containsExactly("orders", "platform");
            WitnessKey orders = keyring.keys().get(0);
            assertThat(new String(orders.secret(), StandardCharsets.UTF_8)).isEqualTo("its-own");
            assertThat(orders.algorithm()).isEqualTo("HmacSHA512");
            assertThat(keyring.keys().get(1).algorithm())
                    .as("silence means the same default the library has")
                    .isEqualTo(MessageAuthenticator.DEFAULT_ALGORITHM);
        }
    }

    @Test
    void a_claimed_name_is_verified_only_by_the_keys_claiming_it() throws IOException {
        settings(String.join("\n",
                "secret.platform.value = general",
                "secret.orders.value = dedicated",
                "secret.orders.applications = orders-*"));

        try (FileConfigStore store = open()) {
            Keyring keyring = store.current().settings().keyring();

            assertThat(keyring.candidatesFor("orders-prod")).extracting(WitnessKey::name)
                    .as("a reservation, not merely a restriction: the general key must not reach a"
                            + " name somebody claimed, or claiming it would buy nothing")
                    .containsExactly("orders");
            assertThat(keyring.candidatesFor("payments")).extracting(WitnessKey::name)
                    .as("and a name nobody claims falls to the keys that claim nothing")
                    .containsExactly("platform");
        }
    }

    @Test
    void a_keyring_with_nothing_unclaimed_cannot_serve_a_new_name() throws IOException {
        settings(String.join("\n",
                "secret.orders.value = dedicated",
                "secret.orders.applications = orders-*"));

        try (FileConfigStore store = open()) {
            Keyring keyring = store.current().settings().keyring();

            assertThat(keyring.candidatesFor("payments")).isEmpty();
            assertThat(keyring.canServeUnclaimedNames()).isFalse();
            assertThat(keyring.summary())
                    .as("a legitimate lockdown, but indistinguishable from a broken witness unless"
                            + " it says which it is")
                    .contains("all scoped");
        }
    }

    @Test
    void claiming_everything_is_the_same_as_claiming_nothing() throws IOException {
        settings(String.join("\n",
                "secret.platform.value = general",
                "secret.wild.value = also-general",
                "secret.wild.applications = *"));

        try (FileConfigStore store = open()) {
            assertThat(store.current().settings().keyring().candidatesFor("payments"))
                    .as("otherwise one '*' entry silently takes every unscoped key out of use")
                    .extracting(WitnessKey::name)
                    .containsExactly("platform", "wild");
        }
    }

    @Test
    void a_key_secret_can_be_kept_out_of_the_config_file() throws IOException {
        Path secretFile = directory.resolve("orders.key");
        Files.write(secretFile, "from-a-file".getBytes(StandardCharsets.UTF_8));
        settings("secret.orders.value = file:" + secretFile);

        try (FileConfigStore store = open()) {
            assertThat(new String(store.current().settings().keyring().keys().get(0).secret(),
                    StandardCharsets.UTF_8)).isEqualTo("from-a-file");
        }
    }

    @Test
    void a_key_without_a_secret_is_refused() throws IOException {
        settings("secret.orders.algorithm = HmacSHA512");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no secret.orders.value");
    }

    @Test
    void a_pattern_that_could_never_match_a_name_is_refused() throws IOException {
        settings(String.join("\n",
                "secret.orders.value = k",
                "secret.orders.applications = " + new String(new char[33]).replace('\0', 'a')));

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can never match anything");
    }

    @Test
    void an_application_file_carrying_key_material_is_refused_rather_than_ignored() throws IOException {
        settings("secret.platform.value = general");
        application("orders", "secret=its-own");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .as("believing a tenant has its own key when the keyring decides otherwise is the"
                        + " worst failure available here, so it must not start")
                .hasMessageContaining("keys live in witness.properties");
    }

    @Test
    void a_name_too_long_for_the_wire_is_refused_at_load() throws IOException {
        String tooLong = new String(new char[33]).replace('\0', 'a');
        application(tooLong, "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the wire field holds 32");
    }

    @Test
    void a_name_that_could_not_travel_on_the_wire_is_refused() throws IOException {
        application("has space", "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("printable ASCII");
    }

    // ------------------------------------------------------------ timings

    @Test
    void nothing_configured_is_the_lan_profile() throws IOException {
        application("orders", "");

        assertThat(open().current().settings().defaultTimings()).isEqualTo(Timings.lan());
    }

    @Test
    void a_named_profile_is_taken_whole() throws IOException {
        settings("timings.profile=wan");
        application("orders", "");

        assertThat(open().current().settings().defaultTimings()).isEqualTo(Timings.wan());
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
                "timings.advertisement.millis=60000",
                "timings.failure.threshold=5"));
        application("orders", "");

        Timings timings = open().current().settings().defaultTimings();

        assertThat(timings).isEqualTo(Timings.builder()
                .heartbeatInterval(Duration.ofMillis(250))
                .electionTimeoutMin(Duration.ofSeconds(2))
                .electionTimeoutMax(Duration.ofSeconds(4))
                .probeInterval(Duration.ofSeconds(3))
                .advertisementInterval(Duration.ofSeconds(60))
                .failureThreshold(5)
                .build());
    }

    @Test
    void a_profile_and_an_explicit_interval_together_are_refused() throws IOException {
        settings(String.join("\n", "timings.profile=wan", "timings.heartbeat.millis=250"));
        application("orders", "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timings.profile is set to 'wan'")
                .hasMessageContaining("timings.heartbeat.millis")
                .hasMessageContaining("Choose one");
    }

    @Test
    void one_election_bound_without_its_pair_is_refused() throws IOException {
        settings("timings.election.min.millis=2000");
        application("orders", "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without its pair");
    }

    @Test
    void an_incoherent_custom_set_fails_with_the_librarys_own_message() throws IOException {
        settings("timings.heartbeat.millis=100");
        application("orders", "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("electionTimeoutMin must be at least");
    }

    @Test
    void a_duration_that_is_not_a_number_says_which_key() throws IOException {
        settings("timings.heartbeat.millis=250ms");
        application("orders", "");

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timings.heartbeat.millis must be a whole number of"
                        + " milliseconds");
    }

    /**
     * The reason these moved out of {@code witness.properties}: a witness serves clusters it does
     * not own, and one of them being on another network must not decide the intervals for all of
     * them. Before this, a witness serving a LAN cluster and a WAN one was configured wrongly for
     * whichever it was not — and, since nodes advertise their intervals, said so in an error
     * nobody could act on.
     */
    @Test
    void two_applications_can_run_different_profiles() throws IOException {
        settings("timings.profile=lan");
        application("orders", "");
        application("billing", "timings.profile=wan");

        ConfigSnapshot snapshot = open().current();

        assertThat(snapshot.application("orders").timings()).isEqualTo(Timings.lan());
        assertThat(snapshot.application("billing").timings()).isEqualTo(Timings.wan());
    }

    @Test
    void an_application_that_says_nothing_takes_the_witness_wide_intervals() throws IOException {
        settings("timings.profile=wan");
        application("orders", "");

        assertThat(open().current().application("orders").timings())
                .as("the same fallback the secret and the algorithm already use")
                .isEqualTo(Timings.wan());
    }

    @Test
    void an_application_can_spell_its_intervals_out() throws IOException {
        application("orders", String.join("\n",
                "timings.heartbeat.millis=250",
                "timings.election.min.millis=2000",
                "timings.election.max.millis=4000",
                "timings.probe.millis=3000",
                "timings.advertisement.millis=60000",
                "timings.failure.threshold=5"));

        assertThat(open().current().application("orders").timings()).isEqualTo(Timings.builder()
                .heartbeatInterval(Duration.ofMillis(250))
                .electionTimeoutMin(Duration.ofSeconds(2))
                .electionTimeoutMax(Duration.ofSeconds(4))
                .probeInterval(Duration.ofSeconds(3))
                .advertisementInterval(Duration.ofSeconds(60))
                .failureThreshold(5)
                .build());
    }

    @Test
    void an_applications_profile_and_explicit_interval_together_are_refused() throws IOException {
        application("orders", String.join("\n",
                "timings.profile=wan", "timings.heartbeat.millis=250"));

        assertThatThrownBy(this::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Choose one");
    }

    // ------------------------------------------------------------ hot reload

    @Test
    void a_new_application_is_picked_up_without_a_restart() throws IOException {
        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);
            assertThat(store.current().applications()).isEmpty();

            application("orders", "");
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().applicationNames()).containsExactly("orders");
            assertThat(changes.added).containsExactly("orders");
        }
    }

    @Test
    void a_removed_application_stops_being_served_but_keeps_its_state() throws IOException {
        application("orders", "");
        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);

            Files.delete(applicationFile("orders"));
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().applications()).isEmpty();
            assertThat(changes.removed).containsExactly("orders");
        }
    }

    /**
     * Rotation stopped being a mechanism and became list membership: add the new key, wait, remove
     * the old. Both are simply in the ring while the window is open.
     */
    @Test
    void a_rotation_is_two_keys_in_the_ring_at_once() throws IOException {
        settings("secret.old.value = old");
        try (FileConfigStore store = open()) {
            settings("secret.old.value = old\nsecret.new.value = new");
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().settings().keyring().candidatesFor("orders"))
                    .extracting(WitnessKey::name)
                    .as("nothing has to be restarted in lockstep; both keys verify meanwhile")
                    .containsExactly("new", "old");

            settings("secret.new.value = new");
            assertThat(rescan(store)).isTrue();
            assertThat(store.current().settings().keyring().candidatesFor("orders"))
                    .extracting(WitnessKey::name)
                    .containsExactly("new");
        }
    }

    @Test
    void a_malformed_file_never_takes_down_the_running_configuration() throws IOException {
        application("orders", "");
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
        application("orders", "");
        try (FileConfigStore store = open()) {
            application("bad name", "secret=x");
            rescan(store);
            assertThat(store.current().applicationNames()).containsExactly("orders");

            Files.delete(applicationFile("bad name"));
            application("billing", "");
            rescan(store);

            assertThat(store.current().applicationNames()).containsExactly("billing", "orders");
        }
    }

    /**
     * Onboarding a tenant, or moving one to another network, must not need a witness restart —
     * that is the one genuinely risky operation in its lifecycle, since every two-node cluster it
     * serves tolerates zero failures while it is down.
     */
    @Test
    void an_applications_intervals_can_be_changed_without_a_restart() throws IOException {
        application("orders", "");
        try (FileConfigStore store = open()) {
            assertThat(store.current().application("orders").timings()).isEqualTo(Timings.lan());

            application("orders", "timings.profile=wan");
            assertThat(rescan(store)).isTrue();

            assertThat(store.current().application("orders").timings()).isEqualTo(Timings.wan());
        }
    }

    @Test
    void an_unchanged_directory_reports_no_change() throws IOException {
        application("orders", "");
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
            application("orders", "");

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

    @Test
    void a_settings_only_change_is_published() throws IOException {
        settings("config.source=file\nconfig.cluster.limit=7");
        application("orders", "");

        try (FileConfigStore store = open()) {
            assertThat(store.current().settings().configSource())
                    .isEqualTo(ConfigSource.FILE);

            // Only witness.properties moves. The served set is byte-for-byte what it was, which is
            // exactly the case a reload used to read from disk and then throw away.
            settings("config.source=cluster\nconfig.cluster.limit=9");

            assertThat(rescan(store))
                    .as("a settings-only edit is a change, even with the applications untouched")
                    .isTrue();
            assertThat(store.current().settings().configSource())
                    .isEqualTo(ConfigSource.CLUSTER);
            assertThat(store.current().settings().clusterApplicationLimit()).isEqualTo(9);
        }
    }

    @Test
    void a_settings_only_change_does_not_disturb_the_applications() throws IOException {
        settings("config.source=file");
        application("orders", "");

        Recording changes = new Recording();
        try (FileConfigStore store = open()) {
            store.addListener(changes);
            ApplicationConfig before = store.current().application("orders");

            settings("config.source=cluster");
            assertThat(rescan(store)).isTrue();

            assertThat(changes.added).isEmpty();
            assertThat(changes.changed).isEmpty();
            assertThat(changes.removed).isEmpty();
            assertThat(store.current().application("orders"))
                    .as("no tenant is rebuilt for a change that was not theirs")
                    .isEqualTo(before);
        }
    }

    @Test
    void a_restart_required_key_is_still_published_so_the_snapshot_stays_truthful()
            throws IOException {
        settings("bind.port=9999");

        try (FileConfigStore store = open()) {
            settings("bind.port=8888");

            assertThat(rescan(store)).isTrue();
            // The socket is bound once and does not move; what must not happen is the snapshot
            // and the file disagreeing with nothing anywhere saying which one is in force. The
            // WARN naming it is not asserted here: capturing SLF4J output needs a recording
            // backend, and that is a test dependency for one assertion.
            assertThat(store.current().settings().bindAddress().getPort()).isEqualTo(8888);
        }
    }

    @Test
    void a_reload_that_changes_nothing_is_not_a_change() throws IOException {
        settings("config.source=cluster");
        application("orders", "");

        try (FileConfigStore store = open()) {
            settings("config.source=cluster");
            application("orders", "");

            assertThat(rescan(store))
                    .as("rewriting the same values must not rebuild a tenant's node")
                    .isFalse();
        }
    }

    @Test
    void an_empty_applications_directory_is_the_intended_shape_under_cluster() throws IOException {
        settings("config.source=cluster\nsecret.platform.value = k");

        try (FileConfigStore store = open()) {
            assertThat(store.current().applications())
                    .as("applications arrive from their own nodes, so there is nothing to configure")
                    .isEmpty();
            assertThat(store.current().settings().configSource()).isEqualTo(ConfigSource.CLUSTER);
        }
    }

    /**
     * A witness in full cluster mode needs exactly one file. Keys are in the keyring, timings come
     * from the clusters, applications create themselves — so the per-tenant directory that cannot
     * fit a single Kubernetes ConfigMap need not exist at all.
     */
    @Test
    void the_applications_directory_need_not_exist_at_all() throws IOException {
        Files.delete(directory.resolve("applications"));
        settings("config.source=cluster\nsecret.platform.value = k");

        try (FileConfigStore store = open()) {
            assertThat(store.current().applications()).isEmpty();
            assertThat(store.current().settings().keyring().keys()).hasSize(1);
        }
    }

    // ------------------------------------------------------- detecting a change

    @Test
    void a_directory_that_has_not_moved_is_not_re_read() throws IOException {
        settings("secret.platform.value = k");
        application("orders", "");

        try (FileConfigStore store = open()) {
            Recording changes = new Recording();
            store.addListener(changes);

            for (int tick = 0; tick < 5; tick++) {
                assertThat(rescan(store))
                        .as("the steady state must cost a checksum, not a parse of every file")
                        .isFalse();
            }
            assertThat(changes.added).isEmpty();
            assertThat(changes.changed).isEmpty();
        }
    }

    @Test
    void an_edit_of_the_same_length_is_still_noticed() throws IOException {
        settings("bind.port=9999");

        try (FileConfigStore store = open()) {
            // Identical length, and quite possibly the same filesystem timestamp. This is an
            // ordinary edit, and the first version of the change detector could not see it.
            settings("bind.port=8888");

            assertThat(rescan(store)).isTrue();
            assertThat(store.current().settings().bindAddress().getPort()).isEqualTo(8888);
        }
    }

    /**
     * The case that silently defeats {@code WatchService}, and the reason it is gone.
     *
     * <p>A Kubernetes ConfigMap volume is a symlink farm: an update writes a new timestamped
     * directory, points {@code ..data} at it and deletes the old one. A watch registered on the
     * resolved directory is then watching something deleted — it never fires again and nothing
     * says so. Reading through the link sees the new bytes like any other edit.
     */
    @Test
    void a_swapped_symlink_target_is_noticed_the_way_a_config_map_updates() throws IOException {
        Path real = directory.resolve("..2026_09_02_10_00_00");
        Files.createDirectories(real);
        Files.write(real.resolve("witness.properties"),
                "bind.port=9999".getBytes(StandardCharsets.UTF_8));
        Path data = directory.resolve("..data");
        try {
            Files.createSymbolicLink(data, real);
            Files.deleteIfExists(directory.resolve("witness.properties"));
            Files.createSymbolicLink(directory.resolve("witness.properties"),
                    data.resolve("witness.properties"));
        } catch (UnsupportedOperationException | IOException e) {
            // A filesystem without symlinks has nothing to prove here.
            return;
        }

        try (FileConfigStore store = open()) {
            assertThat(store.current().settings().bindAddress().getPort()).isEqualTo(9999);

            Path replacement = directory.resolve("..2026_09_02_11_00_00");
            Files.createDirectories(replacement);
            Files.write(replacement.resolve("witness.properties"),
                    "bind.port=8888".getBytes(StandardCharsets.UTF_8));
            Files.delete(data);
            Files.createSymbolicLink(data, replacement);

            assertThat(rescan(store))
                    .as("the kubelet swaps a symlink rather than writing the file, which a watch on"
                            + " the resolved directory never sees")
                    .isTrue();
            assertThat(store.current().settings().bindAddress().getPort()).isEqualTo(8888);
        }
    }

    // --------------------------------------------------------------- helpers

    private FileConfigStore open() {
        return new FileConfigStore(directory, clock);
    }

    /** Advances past the check interval and runs one, which is the whole reload mechanism. */
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
