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
                "secret=" + SECRET,
                "timings.heartbeat.millis=20",
                "timings.election.min.millis=100",
                "timings.election.max.millis=200",
                "timings.probe.millis=100",
                "timings.advertisement.millis=100",
                "timings.failure.threshold=3"));
        applicationWith("billing", String.join("\n",
                "secret=" + SECRET,
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

            WitnessClient.Reply toOrders = orders.awaitTimings(server);
            WitnessClient.Reply toBilling = billing.awaitTimings(server);

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
    void permissive_mode_serves_an_application_nobody_configured() {
        settings("registration.mode=permissive\nsecret=" + SECRET);
        start();

        try (WitnessClient stranger = client("payments", 1)) {
            assertThat(stranger.requestVote(server, 1L)).isTrue();
        }

        assertThat(server.servedApplications()).containsExactly("payments");
        assertThat(server.routing().autoRegistrations()).isEqualTo(1L);
        assertThat(server.votedFor("payments")).isEqualTo(1);
    }

    @Test
    void permissive_mode_stops_auto_registering_at_its_limit() {
        settings("registration.mode=permissive\nregistration.limit=2\nsecret=" + SECRET);
        start();

        for (String name : new String[] {"one", "two", "three", "four"}) {
            try (WitnessClient stranger = client(name, 1)) {
                stranger.fireAndForget(server, 1L);
            }
        }

        assertThat(server.servedApplications())
                .as("an unauthenticated witness must not be fillable by whoever can reach the port")
                .containsExactly("one", "two");
        assertThat(server.routing().autoRegistrationsRefused()).isEqualTo(2L);
    }

    @Test
    void a_name_that_could_choose_where_its_state_is_written_is_refused() {
        settings("registration.mode=permissive\nsecret=" + SECRET);
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
    void one_applications_secret_does_not_authenticate_another() {
        applicationWith("orders", "secret=orders-secret");
        applicationWith("billing", "secret=billing-secret");
        start();

        try (WitnessClient crossTalk = new WitnessClient("billing", 1, "orders-secret",
                WITNESS_ID, server.boundAddress())) {
            assertThat(crossTalk.requestVote(server, 1L))
                    .as("per-application secrets are what make multi-tenancy isolation real rather"
                            + " than nominal")
                    .isNull();
        }
        assertThat(server.currentTerm("billing")).isZero();
    }

    @Test
    void a_rotated_secret_accepts_both_until_the_previous_one_is_removed() {
        applicationWith("orders", "secret=new-secret\nsecret.previous=" + SECRET);
        start();

        // Mid-rotation, every party holds both: the witness tags its replies with the current
        // secret, so a node that only had the old one could send but never hear an answer.
        try (WitnessClient onTheOldSecret = new WitnessClient("orders", 1, SECRET, "new-secret",
                WITNESS_ID, server.boundAddress());
             WitnessClient onTheNewSecret = new WitnessClient("orders", 2, "new-secret", SECRET,
                     WITNESS_ID, server.boundAddress())) {

            assertThat(onTheOldSecret.requestVote(server, 1L))
                    .as("rotation is two file edits with a window, not a flag day across every node")
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
        assertThat(server.stats().get("orders").role().name())
                .as("a witness is an ordinary voter that declines to campaign, and that is the"
                        + " whole of the difference")
                .isEqualTo("FOLLOWER");
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
        applicationWith(name, "secret=" + SECRET);
    }

    private void applicationWith(String name, String contents) {
        write(directory.resolve("applications/" + name + ".properties"), contents);
    }

    private void settings(String contents) {
        write(directory.resolve("witness.properties"), contents);
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
