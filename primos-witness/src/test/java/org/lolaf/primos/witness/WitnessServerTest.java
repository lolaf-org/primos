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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.Role;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.time.MonotonicClock;

/**
 * The multi-tenant server: routing by application name, registration policy, and the isolation
 * that stops one cluster's vote being spent by another.
 */
class WitnessServerTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";

    @TempDir
    Path directory;

    private WitnessServer server;

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        // Keys live in witness.properties now, not in an application's own file, so the shared one
        // is written for every test; those that care about which key covers what overwrite it.
        settings("");
    }

    @AfterEach
    void closeServer() {
        if (server != null) {
            server.close();
        }
    }

    // ------------------------------------------------- per-application timings

    /**
     * The whole point of moving the timing keys out of {@code witness.properties}: a witness serves
     * clusters it does not own, and one of them being on another network must not decide the
     * intervals for all of them.
     *
     * <p>Asserted through the advertisement rather than by reaching inside the server, because that
     * is the mechanism this exists to serve — a node compares what the witness sends against its
     * own, and before this change one of these two tenants would always have been told the wrong
     * thing, in an error nobody could act on.
     */
    @Test
    void each_application_is_served_with_its_own_intervals() {
        applicationWith("orders", String.join("\n",
                "timings.heartbeat.millis=20",
                "timings.election.min.millis=100",
                "timings.election.max.millis=200",
                "timings.probe.millis=100",
                "timings.advertisement.millis=100",
                "timings.failure.threshold=3"));
        applicationWith("billing", String.join("\n",
                "timings.heartbeat.millis=40",
                "timings.election.min.millis=400",
                "timings.election.max.millis=800",
                "timings.probe.millis=200",
                "timings.advertisement.millis=200",
                "timings.failure.threshold=5"));
        start();

        try (WitnessClient orders = client("orders", 1);
             WitnessClient billing = client("billing", 2)) {
            // The witness answers only senders it has heard from, so speak first.
            orders.heartbeat(server, 1);
            billing.heartbeat(server, 1);

            WitnessClient.Reply toOrders = orders.awaitNodeConfig(server);
            WitnessClient.Reply toBilling = billing.awaitNodeConfig(server);

            assertThat(toOrders).as("the witness never advertised to 'orders'").isNotNull();
            assertThat(toOrders.heartbeatMillis).isEqualTo(20);
            assertThat(toOrders.electionMinMillis).isEqualTo(100);
            assertThat(toOrders.failureThreshold).isEqualTo(3);

            assertThat(toBilling).as("the witness never advertised to 'billing'").isNotNull();
            assertThat(toBilling.heartbeatMillis)
                    .as("one tenant's profile must not be every tenant's")
                    .isEqualTo(40);
            assertThat(toBilling.electionMinMillis).isEqualTo(400);
            assertThat(toBilling.failureThreshold).isEqualTo(5);
        }
    }

    // ------------------------------------------------- per-application isolation

    @Test
    void one_applications_spent_vote_is_not_spent_for_another() {
        application("orders");
        application("billing");
        start();

        try (WitnessClient orders = client("orders", 1);
             WitnessClient billing = client("billing", 2)) {

            assertThat(orders.requestVote(server, 4L)).isTrue();
            assertThat(server.votedFor("orders")).isEqualTo(1);

            // Same term, different application, different candidate. If the two shared a bucket
            // this would be refused, because the vote for term 4 would already be spent. Sharing
            // one is not an inconvenience: it is a double vote and therefore two leaders.
            assertThat(billing.requestVote(server, 4L))
                    .as("each application's term and vote are its own")
                    .isTrue();
            assertThat(server.votedFor("billing")).isEqualTo(2);
            assertThat(server.votedFor("orders"))
                    .as("and granting one leaves the other untouched")
                    .isEqualTo(1);
        }
    }

    @Test
    void a_second_candidate_in_the_same_application_and_term_is_refused() {
        application("orders");
        start();

        try (WitnessClient first = client("orders", 1);
             WitnessClient second = client("orders", 2)) {

            assertThat(first.requestVote(server, 7L)).isTrue();
            assertThat(second.requestVote(server, 7L))
                    .as("one vote per term, which is the whole reason the witness is durable")
                    .isFalse();
            assertThat(server.votedFor("orders")).isEqualTo(1);
        }
    }

    @Test
    void each_application_keeps_its_own_state_directory() {
        application("orders");
        application("billing");
        start();

        try (WitnessClient orders = client("orders", 1);
             WitnessClient billing = client("billing", 2)) {
            orders.requestVote(server, 3L);
            billing.requestVote(server, 9L);
        }

        assertThat(directory.resolve("state/orders/state")).exists();
        assertThat(directory.resolve("state/billing/state")).exists();
        assertThat(server.currentTerm("orders")).isEqualTo(3L);
        assertThat(server.currentTerm("billing"))
                .as("a term is per application, so one cluster racing ahead does not drag another")
                .isEqualTo(9L);
    }

    @Test
    void a_vote_survives_a_restart_of_the_witness() {
        application("orders");
        start();
        try (WitnessClient candidate = client("orders", 1)) {
            assertThat(candidate.requestVote(server, 12L)).isTrue();
        }
        server.close();

        start();
        try (WitnessClient other = client("orders", 2)) {
            assertThat(other.requestVote(server, 12L))
                    .as("a witness that forgot a vote across a restart would grant a second one in"
                            + " the same term, which is the split brain it exists to prevent")
                    .isFalse();
        }
        assertThat(server.votedFor("orders")).isEqualTo(1);
    }

    // ------------------------------------------------------------- registration

    @Test
    void strict_mode_drops_an_application_nobody_configured() {
        application("orders");
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            stranger.fireAndForget(server, 1L);
        }

        assertThat(server.servedApplications()).containsExactly("orders");
        assertThat(server.routing().unservedApplication()).isEqualTo(1L);
        assertThat(server.routing().autoRegistrations()).isZero();
    }

    @Test
    void cluster_mode_serves_an_application_nobody_configured() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            stranger.announceConfig(server);
            assertThat(stranger.requestVote(server, 1L)).isTrue();
        }

        assertThat(server.servedApplications()).containsExactly("payments");
        assertThat(server.routing().autoRegistrations()).isEqualTo(1L);
        assertThat(server.votedFor("payments")).isEqualTo(1);
    }

    @Test
    void cluster_mode_stops_creating_applications_at_its_limit() {
        settings("config.source=cluster\nconfig.cluster.limit=2\nsecret.shared.value=" + SECRET);
        start();

        for (String name : new String[] {"one", "two", "three", "four"}) {
            try (WitnessClient stranger = client(name, 1)) {
                stranger.announceConfig(server);
            }
        }

        assertThat(server.servedApplications())
                .as("an unauthenticated witness must not be fillable by whoever can reach the port")
                .containsExactly("one", "two");
        assertThat(server.routing().autoRegistrationsRefused()).isEqualTo(2L);
    }

    @Test
    void cluster_mode_does_not_create_an_application_from_a_vote_request() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            // A vote is the most consequential thing it could ask for, and the witness has no
            // timings to answer it by until the cluster has said what they are.
            assertThat(stranger.requestVote(server, 1L))
                    .as("an unserved application gets no answer until it states its configuration")
                    .isNull();
        }

        assertThat(server.servedApplications()).isEmpty();
        assertThat(server.routing().autoRegistrations()).isZero();
        assertThat(server.routing().awaitingNodeConfig()).isEqualTo(1L);
    }

    @Test
    void cluster_mode_creates_the_application_once_its_config_arrives() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            assertThat(stranger.requestVote(server, 1L)).isNull();
            assertThat(server.servedApplications()).isEmpty();

            // The pair to the assertion above: the same node, the same witness, one config
            // message in between. Without this the test above would pass on a broken witness
            // that served nobody at all.
            stranger.announceConfig(server);

            assertThat(server.servedApplications()).containsExactly("payments");
            assertThat(stranger.requestVote(server, 1L)).isTrue();
        }
    }

    @Test
    void file_mode_creates_nothing_whatever_the_message_is() {
        settings("config.source=file\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            stranger.announceConfig(server);
        }

        assertThat(server.servedApplications())
                .as("a config message is not a way past an operator's explicit tenant list")
                .isEmpty();
        assertThat(server.routing().unservedApplication()).isEqualTo(1L);
        assertThat(server.routing().autoRegistrations()).isZero();
    }

    @Test
    void cluster_mode_follows_the_intervals_a_cluster_states() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient orders = client("orders", 1)) {
            orders.announceConfig(server, 200, 1500, 3000, 2000, 4);

            // Asserted through the witness's own advertisement rather than by reaching into the
            // server: what matters is that the cluster is told the witness now runs its profile.
            WitnessClient.Reply back = orders.awaitNodeConfig(server);

            assertThat(back).as("the witness never advertised back").isNotNull();
            assertThat(back.heartbeatMillis).isEqualTo(200);
            assertThat(back.electionMinMillis).isEqualTo(1500);
            assertThat(back.electionMaxMillis).isEqualTo(3000);
            assertThat(back.probeMillis).isEqualTo(2000);
            assertThat(back.failureThreshold).isEqualTo(4);
        }
    }

    @Test
    void file_mode_ignores_the_intervals_a_cluster_states() {
        settings(String.join("\n", "config.source=file", "secret.shared.value=" + SECRET,
                "timings.heartbeat.millis=20",
                "timings.election.min.millis=100",
                "timings.election.max.millis=200",
                "timings.probe.millis=100",
                "timings.advertisement.millis=100",
                "timings.failure.threshold=3"));
        application("orders");
        start();

        try (WitnessClient orders = client("orders", 1)) {
            orders.announceConfig(server, 200, 1500, 3000, 2000, 4);

            WitnessClient.Reply back = orders.awaitNodeConfig(server);

            assertThat(back).isNotNull();
            assertThat(back.heartbeatMillis)
                    .as("an operator who forbade automatic change must not be overruled by a"
                            + " datagram; the mismatch is reported and nothing else")
                    .isEqualTo(20);
            assertThat(back.electionMinMillis).isEqualTo(100);
        }
    }

    @Test
    void two_clusters_stating_different_intervals_each_get_their_own() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient orders = client("orders", 1);
             WitnessClient billing = client("billing", 2)) {
            orders.announceConfig(server, 20, 100, 200, 100, 3);
            billing.announceConfig(server, 200, 1500, 3000, 2000, 5);

            WitnessClient.Reply toOrders = orders.awaitNodeConfig(server);
            WitnessClient.Reply toBilling = billing.awaitNodeConfig(server);

            assertThat(toOrders).isNotNull();
            assertThat(toBilling).isNotNull();
            assertThat(toOrders.heartbeatMillis)
                    .as("following one tenant must not retune every other tenant")
                    .isEqualTo(20);
            assertThat(toBilling.heartbeatMillis).isEqualTo(200);
            assertThat(toBilling.failureThreshold).isEqualTo(5);
        }
    }

    @Test
    void intervals_that_do_not_hold_together_are_ignored_rather_than_adopted() {
        // The advertisement interval is not on the wire, so a witness keeps its own — and this
        // test needs two of them. Both profiles here are deliberately fast so it does not spend
        // thirty seconds waiting for the second.
        settings(String.join("\n", "config.source=cluster", "secret.shared.value=" + SECRET,
                "timings.heartbeat.millis=20",
                "timings.election.min.millis=100",
                "timings.election.max.millis=200",
                "timings.probe.millis=100",
                "timings.advertisement.millis=200",
                "timings.failure.threshold=3"));
        start();

        try (WitnessClient orders = client("orders", 1)) {
            orders.announceConfig(server, 20, 100, 200, 100, 3);
            assertThat(orders.awaitNodeConfig(server)).isNotNull();

            // 300ms is below the five-heartbeat floor for a 200ms heartbeat, so this set could
            // never have been built by a running node and is not one this witness will adopt.
            orders.announceConfig(server, 200, 300, 600, 1000, 3);

            WitnessClient.Reply back = orders.awaitNodeConfig(server);

            assertThat(back).isNotNull();
            assertThat(back.heartbeatMillis)
                    .as("an unusable profile leaves the last good one in force")
                    .isEqualTo(20);
            assertThat(back.electionMinMillis).isEqualTo(100);
        }
    }

    @Test
    void a_forged_config_message_creates_no_application() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient impostor = new WitnessClient("payments", 1, "not-the-secret",
                WITNESS_ID, server.boundAddress())) {
            impostor.announceConfig(server);
        }

        assertThat(server.servedApplications())
                .as("creating an application is the one consequential thing that happens before a"
                        + " datagram reaches a node, so it must not happen on unverified bytes")
                .isEmpty();
        assertThat(server.routing().registrationBadTag()).isEqualTo(1L);
        assertThat(server.routing().autoRegistrations()).isZero();
    }

    /**
     * Without this the test above would pass on a witness that had simply stopped registering
     * anybody, which is the same outage wearing the fix's clothes.
     */
    @Test
    void a_signed_config_message_still_creates_one() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient member = client("payments", 1)) {
            member.announceConfig(server);
        }

        assertThat(server.servedApplications()).containsExactly("payments");
        assertThat(server.routing().registrationBadTag()).isZero();
    }

    /**
     * The keyring's reservation rule reaching registration, which is the whole point of doing this
     * with the candidate keys rather than with every key the witness holds.
     */
    @Test
    void a_general_key_cannot_create_a_name_another_key_claims() {
        settings(String.join("\n", "config.source=cluster",
                "secret.general.value = " + SECRET,
                "secret.orders.value = orders-secret",
                "secret.orders.applications = orders-*"));
        start();

        try (WitnessClient withTheGeneralKey = client("orders-prod", 1)) {
            withTheGeneralKey.announceConfig(server);
        }
        assertThat(server.servedApplications())
                .as("a reserved name must not be brought into existence by the fleet key")
                .isEmpty();

        try (WitnessClient withTheRightKey = new WitnessClient("orders-prod", 1, "orders-secret",
                WITNESS_ID, server.boundAddress())) {
            withTheRightKey.announceConfig(server);
        }
        assertThat(server.servedApplications()).containsExactly("orders-prod");
    }

    /**
     * The configuration that has always been documented as never to run still behaves as it did:
     * with nothing to verify against, there is nothing this check can do.
     */
    @Test
    void a_witness_with_no_keys_still_registers_whatever_arrives() {
        // Written directly: the settings() helper adds the shared key, and this test is precisely
        // about a witness that has none.
        write(directory.resolve("witness.properties"), "config.source=cluster");
        start();

        try (WitnessClient anybody = new WitnessClient("payments", 1, null,
                WITNESS_ID, server.boundAddress())) {
            anybody.announceConfig(server);
        }

        assertThat(server.servedApplications()).containsExactly("payments");
    }

    @Test
    void a_name_that_could_choose_where_its_state_is_written_is_refused() {
        settings("config.source=cluster\nsecret.shared.value=" + SECRET);
        start();

        try (WitnessClient traversal = client("../../etc", 1)) {
            traversal.fireAndForget(server, 1L);
        }

        assertThat(server.servedApplications()).isEmpty();
        assertThat(server.routing().unusableName()).isEqualTo(1L);
    }

    @Test
    void an_application_that_is_no_longer_configured_keeps_its_state() throws IOException {
        application("orders");
        start();
        try (WitnessClient candidate = client("orders", 1)) {
            assertThat(candidate.requestVote(server, 5L)).isTrue();
        }

        Files.delete(directory.resolve("applications/orders.properties"));
        reloadConfiguration();

        assertThat(server.servedApplications()).isEmpty();
        assertThat(directory.resolve("state/orders/state"))
                .as("a cluster that comes back later still needs the vote it was given remembered;"
                        + " deleting it is the double-vote bug")
                .exists();
    }

    @Test
    void an_application_added_while_running_is_served_without_a_restart() {
        start();
        assertThat(server.servedApplications()).isEmpty();

        application("orders");
        reloadConfiguration();

        assertThat(server.servedApplications()).containsExactly("orders");
        try (WitnessClient candidate = client("orders", 1)) {
            assertThat(candidate.requestVote(server, 1L)).isTrue();
        }
    }

    // ------------------------------------------------------------ authentication

    @Test
    void a_datagram_tagged_with_the_wrong_secret_is_not_answered() {
        application("orders");
        start();

        try (WitnessClient impostor = new WitnessClient("orders", 1, "not-the-secret",
                WITNESS_ID, server.boundAddress())) {
            assertThat(impostor.requestVote(server, 1L))
                    .as("a forged vote grant plus a candidate's own vote is a majority in a"
                            + " three-voter cluster, so this has to be silence rather than a reply")
                    .isNull();
        }
        assertThat(server.currentTerm("orders")).isZero();
    }

    @Test
    void a_key_scoped_to_one_application_does_not_authenticate_another() {
        settings(String.join("\n",
                "secret.orders.value = orders-secret",
                "secret.orders.applications = orders",
                "secret.billing.value = billing-secret",
                "secret.billing.applications = billing"));
        applicationWith("orders", "");
        applicationWith("billing", "");
        start();

        try (WitnessClient crossTalk = new WitnessClient("billing", 1, "orders-secret",
                WITNESS_ID, server.boundAddress())) {
            assertThat(crossTalk.requestVote(server, 1L))
                    .as("claiming a name is what makes isolation real rather than nominal: the"
                            + " other tenant's key must not reach it")
                    .isNull();
        }
        assertThat(server.currentTerm("billing")).isZero();
    }

    @Test
    void an_unscoped_key_cannot_reach_a_name_another_key_claims() {
        settings(String.join("\n",
                "secret.general.value = " + SECRET,
                "secret.orders.value = orders-secret",
                "secret.orders.applications = orders-*"));
        applicationWith("orders-prod", "");
        start();

        try (WitnessClient withTheGeneralKey = new WitnessClient("orders-prod", 1, SECRET,
                WITNESS_ID, server.boundAddress())) {
            assertThat(withTheGeneralKey.requestVote(server, 1L))
                    .as("a claim is a reservation, not merely a restriction; otherwise a dedicated"
                            + " key would buy its tenant nothing")
                    .isNull();
        }
    }

    @Test
    void a_rotated_secret_accepts_both_until_the_previous_one_is_removed() {
        settings("secret.new.value = new-secret\nsecret.old.value = " + SECRET);
        applicationWith("orders", "");
        start();

        // Mid-rotation, every party holds both: the witness signs each reply with whichever key
        // verified the request, so a node holding only one of them still hears an answer.
        try (WitnessClient onTheOldSecret = new WitnessClient("orders", 1, SECRET, "new-secret",
                WITNESS_ID, server.boundAddress());
             WitnessClient onTheNewSecret = new WitnessClient("orders", 2, "new-secret", SECRET,
                     WITNESS_ID, server.boundAddress())) {

            assertThat(onTheOldSecret.requestVote(server, 1L))
                    .as("rotation is a key added and later removed, not a flag day across nodes")
                    .isTrue();
            assertThat(onTheNewSecret.requestVote(server, 2L)).isTrue();
        }
    }

    // ------------------------------------------------------------------- storage

    @Test
    void a_state_store_that_does_not_survive_a_crash_is_refused() {
        application("orders");

        assertThatThrownBy(() -> WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(loopback())
                .stateStores(name -> new MemoryStateStore())
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not survive a crash");
    }

    @Test
    void a_second_witness_on_the_same_state_directory_refuses_to_start() {
        application("orders");
        start();

        // The mechanical backstop behind the operator rule "never run two instances for one
        // identity". Two witnesses answering for one cluster is a double vote by construction, so
        // sharing a state directory has to fail loudly rather than quietly work.
        assertThatThrownBy(() -> WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(loopback())
                .build())
                .hasMessageContaining("already using the state directory");
    }

    @Test
    void a_witness_serving_normally_never_fails_to_send_a_reply() {
        application("orders");
        start();

        try (WitnessClient candidate = client("orders", 1)) {
            assertThat(candidate.requestVote(server, 1L)).isTrue();
        }

        assertThat(server.routing().undeliverableReplies())
                .as("a witness that silently fails to answer is indistinguishable from one nobody"
                        + " is asking, and the cluster waiting for the vote is what pays")
                .isZero();
        assertThat(server.routing().inboxOverflowed()).isZero();
    }

    // ------------------------------------------------------------------ the role

    @Test
    void the_witness_never_becomes_a_candidate_or_a_leader() {
        application("orders");
        start();

        try (WitnessClient candidate = client("orders", 1)) {
            candidate.requestVote(server, 1L);
            // Far longer than any election timeout. A voter that campaigned would have done it by
            // now; this one has no election timer at all.
            for (int i = 0; i < 200; i++) {
                server.runOnce(java.time.Duration.ofMillis(5).toNanos());
            }
        }
        assertThat(server.role("orders"))
                .as("a witness is an ordinary voter that declines to campaign, and that is the"
                        + " whole of the difference")
                .isEqualTo(Role.FOLLOWER);
    }

    // ---------------------------------------------------------------- plumbing

    private void start() {
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(loopback())
                .build();
    }

    /** Waits out the reload debounce and drives the loop until the change has been applied. */
    private void reloadConfiguration() {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            server.runOnce(java.time.Duration.ofMillis(20).toNanos());
        }
    }

    private WitnessClient client(String application, int nodeId) {
        return new WitnessClient(application, nodeId, SECRET, WITNESS_ID, server.boundAddress());
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress("127.0.0.1", 0);
    }

    private void application(String name) {
        applicationWith(name, "");
    }

    private void applicationWith(String name, String contents) {
        write(directory.resolve("applications/" + name + ".properties"), contents);
    }

    /** Carries the shared key unless the test is saying something specific about keys. */
    private void settings(String contents) {
        String withKey = contents.contains("secret.")
                ? contents
                : "secret.shared.value=" + SECRET + "\n" + contents;
        write(directory.resolve("witness.properties"), withKey);
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
