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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.*;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.store.StateStoreException;
import org.lolaf.primos.time.MonotonicClock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a witness reports about itself: counters per served application, and which tenants it has
 * stopped serving.
 *
 * <p>Both exist for something outside this artifact to read — {@code primos-micrometer} for the
 * first and a health endpoint for the second — and both have a failure that is silent. Counters
 * summed across tenants are a number nobody can act on; a quarantined tenant reported as an
 * unhealthy witness takes every other tenant's witness away with it.
 */
class WitnessObservabilityTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";
    /**
     * One recorder per application name, created on demand, so a test can read either.
     */
    private final Map<String, RecordingStats> recorders = new LinkedHashMap<>();
    private final AtomicInteger factoryCalls = new AtomicInteger();
    @TempDir
    Path directory;
    private WitnessServer server;

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        settings();
    }

    @AfterEach
    void closeServer() {
        if (server != null) {
            server.close();
        }
    }

    // ------------------------------------------------------ counters, per tenant

    /**
     * The reason the SPI is a function of the name rather than one sink: a witness runs an election
     * per served cluster, so one tenant's dropped datagrams added to another's would be a number
     * nobody could act on — and it would look perfectly plausible on a dashboard.
     */
    @Test
    void counters_are_recorded_against_the_application_that_produced_them() {
        application("orders");
        application("billing");
        start();

        // A real datagram carrying a tag from the wrong key. No correct sender sends this, which
        // is the point: it is the traffic a counter exists to make visible.
        try (WitnessClient forged = new WitnessClient("orders", 1, "the-wrong-secret", WITNESS_ID,
                server.boundAddress())) {
            forged.fireAndForget(server, 1L);
        }

        assertThat(recorder("orders").drops(DropReason.BAD_TAG))
                .as("the application the datagram named is the one whose counter moves")
                .isEqualTo(1);
        assertThat(recorder("billing").drops(DropReason.BAD_TAG))
                .as("a tenant that saw nothing reports nothing")
                .isZero();
    }

    /**
     * A node is rebuilt whenever its configuration changes, and a binder that registers its meters
     * when it is created would register them a second time if it were asked for again. So the sink
     * is resolved once per application and handed to every node built for it.
     */
    @Test
    void the_sink_is_asked_for_once_per_application_and_survives_a_rebuild() {
        applicationWith("orders", "timings.profile=lan\n");
        start();
        forgeOneDatagram();

        applicationWith("orders", "timings.profile=wan\n");
        reloadConfiguration();
        forgeOneDatagram();

        assertThat(factoryCalls.get())
                .as("asked once when the application started being served, and not again for the"
                        + " node that replaced the first one")
                .isEqualTo(1);
        assertThat(recorder("orders").drops(DropReason.BAD_TAG))
                .as("the rebuilt node reports to the same sink, so a counter does not restart at"
                        + " zero every time an operator edits a file")
                .isEqualTo(2);
    }

    /**
     * The SPI already promises that a listener which throws is caught and logged rather than
     * killing the node. On a witness that promise is worth more than it is anywhere else: the
     * catch-all it would otherwise reach marks the node {@code FAILED}, and a failed node here is a
     * quarantined tenant — so a bug in a counter would take a cluster's tie-breaker away from it.
     */
    @Test
    void a_listener_that_throws_does_not_cost_its_tenant_the_witness() {
        application("orders");
        server = builder()
                .statsListeners(application -> new PrimosStatsListener() {
                    @Override
                    public void onDatagramDropped(DropReason reason) {
                        throw new IllegalStateException("a bug in somebody's metrics binder");
                    }
                })
                .build();

        forgeOneDatagram();

        assertThat(server.quarantinedApplications())
                .as("the guard is applied at the boundary, so the exception never reaches the"
                        + " node's catch-all")
                .isEmpty();
        try (WitnessClient voter = new WitnessClient("orders", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            assertThat(voter.requestVote(server, 1L))
                    .as("and the tenant still gets the vote it came for")
                    .isTrue();
        }
    }

    /**
     * The same rule applied one step earlier: the call that produces a listener is as much part of
     * an embedding application as the listener is, and it must not be able to stop a witness
     * serving either.
     */
    @Test
    void a_factory_that_throws_does_not_stop_the_witness_serving() {
        application("orders");
        server = builder()
                .statsListeners(application -> {
                    throw new IllegalStateException("a bug in somebody's wiring");
                })
                .build();

        try (WitnessClient voter = new WitnessClient("orders", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            assertThat(voter.requestVote(server, 1L))
                    .as("served without counters, rather than not served")
                    .isTrue();
        }
    }

    // -------------------------------------------------------------- quarantine

    @Test
    void a_healthy_witness_has_nothing_quarantined() {
        application("orders");
        start();

        try (WitnessClient voter = new WitnessClient("orders", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            voter.requestVote(server, 1L);
        }

        assertThat(server.quarantinedApplications()).isEmpty();
    }

    /**
     * The shape a health endpoint needs. One tenant whose disk can no longer make a vote durable
     * stops being served; every other tenant keeps its tie-breaker, and the witness is still doing
     * its job for all of them — which is why this is a set of names to put in a health detail and
     * not a verdict about the server.
     */
    @Test
    void a_failed_application_is_named_and_the_others_keep_being_served() {
        application("orders");
        application("billing");
        // Durable as far as the server's startup check can tell, and unable to write: the failure
        // this quarantine exists for is a disk that goes bad while the witness is running, not one
        // that was never there.
        server = builder()
                .stateStores(application -> "orders".equals(application)
                        ? new UnwritableStateStore()
                        : new InMemoryDurableStateStore())
                .build();

        try (WitnessClient broken = new WitnessClient("orders", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            // A vote must be persisted before it is granted, so this is the datagram that finds
            // the broken disk.
            broken.fireAndForget(server, 1L);
        }

        assertThat(server.quarantinedApplications())
                .as("named, so a health detail can say which tenant needs somebody")
                .containsExactly("orders");
        try (WitnessClient healthy = new WitnessClient("billing", 1, SECRET, WITNESS_ID,
                server.boundAddress())) {
            assertThat(healthy.requestVote(server, 1L))
                    .as("one broken disk must not be a shared outage")
                    .isTrue();
        }
        assertThat(server.role("billing")).isEqualTo(Role.FOLLOWER);
    }

    // ---------------------------------------------------------------- plumbing

    private void start() {
        server = builder().build();
    }

    private WitnessServer.Builder builder() {
        return WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .statsListeners(this::newRecorder);
    }

    private PrimosStatsListener newRecorder(String application) {
        factoryCalls.incrementAndGet();
        return recorder(application);
    }

    private RecordingStats recorder(String application) {
        return recorders.computeIfAbsent(application, name -> new RecordingStats());
    }

    /**
     * One datagram for 'orders' carrying a tag from the wrong key, which is dropped and counted.
     */
    private void forgeOneDatagram() {
        try (WitnessClient forged = new WitnessClient("orders", 1, "the-wrong-secret", WITNESS_ID,
                server.boundAddress())) {
            forged.fireAndForget(server, 1L);
        }
    }

    /**
     * Waits out the reload debounce and drives the loop until the change has been applied.
     */
    private void reloadConfiguration() {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            server.runOnce(Duration.ofMillis(20).toNanos());
        }
    }

    private void application(String name) {
        applicationWith(name, "");
    }

    private void applicationWith(String name, String contents) {
        write(directory.resolve("applications/" + name + ".properties"), contents);
    }

    private void settings() {
        write(directory.resolve("witness.properties"), "secret.shared.value=" + SECRET + "\n");
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Durable by its own account and unable to write — a disk that has gone bad rather than one
     * that was never asked to be durable, which the server refuses outright at startup.
     */
    private static final class UnwritableStateStore implements StateStore {

        @Override
        public PersistentState load() {
            return PersistentState.fresh();
        }

        @Override
        public void save(long currentTerm, int votedFor) {
            throw new StateStoreException("this disk can no longer make a vote durable");
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    /**
     * What the other tenant runs on. {@code MemoryStateStore} reports itself non-durable and the
     * server refuses it, quite rightly — this one is that lie told deliberately, in a test that is
     * about what happens to the tenants beside a broken one.
     */
    private static final class InMemoryDurableStateStore implements StateStore {

        private PersistentState state = PersistentState.fresh();

        @Override
        public PersistentState load() {
            return state;
        }

        @Override
        public void save(long currentTerm, int votedFor) {
            state = new PersistentState(currentTerm, votedFor);
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
