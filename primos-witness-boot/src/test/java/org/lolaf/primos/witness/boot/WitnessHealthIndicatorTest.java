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
package org.lolaf.primos.witness.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.store.StateStoreException;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.StateStoreFactory;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/**
 * What the health endpoint says, and the one thing it must never say.
 *
 * <p>A witness serving fifty clusters with one quarantined is doing its job for forty-nine.
 * Reporting that as {@code DOWN} takes all fifty out of rotation over one of them — an outage
 * caused by the health check rather than found by it, and the natural implementation, which
 * aggregates over tenants, does exactly that.
 */
class WitnessHealthIndicatorTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";
    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @TempDir
    Path directory;

    private WitnessServer server;
    private WitnessServerLifecycle lifecycle;

    @Test
    void up_while_the_loop_is_running() {
        serve(StateStoreFactory.perApplicationDirectory(directory.resolve("state")));

        Health health = health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("served", server.servedApplications());
        assertThat(health.getDetails().get("quarantined"))
                .as("present even when empty, so an alert is written against a shape that does not"
                        + " change the day something goes wrong")
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.COLLECTION)
                .isEmpty();
    }

    /**
     * The test the class exists for.
     */
    @Test
    void a_quarantined_tenant_is_named_in_the_detail_and_the_witness_stays_up() {
        // 'orders' cannot write its vote; 'billing' is ordinary. The failure a witness quarantines
        // for is a disk that goes bad while it runs, not one that was never durable — the server
        // refuses that outright at startup.
        serve(application -> "orders".equals(application)
                ? new UnwritableStateStore()
                : new InMemoryDurableStateStore());

        try (WitnessTestClient broken = client("orders")) {
            // A vote is persisted before it is granted, so this is the datagram that finds the bad
            // disk. No reply comes back, which is what a quarantined application looks like.
            broken.requestVote(1L, Duration.ofSeconds(1));
        }
        Health health = awaitQuarantineOf("orders");

        assertThat(health.getStatus())
                .as("one broken tenant is not a broken witness")
                .isEqualTo(Status.UP);
        assertThat(health.getDetails().get("quarantined"))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.COLLECTION)
                .containsExactly("orders");
        assertThat(health.getDetails()).containsKey("quarantineReason");

        try (WitnessTestClient healthy = client("billing")) {
            assertThat(healthy.requestVote(1L, PATIENCE))
                    .as("and the other forty-nine keep their tie-breaker")
                    .isTrue();
        }
    }

    @Test
    void down_when_the_loop_is_not_running() {
        serve(StateStoreFactory.perApplicationDirectory(directory.resolve("state")));
        lifecycle.stop();

        Health health = health();

        assertThat(health.getStatus())
                .as("answering HTTP while serving nobody is the one state that is genuinely DOWN")
                .isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("reason").toString()).contains("serving nobody");
    }

    // ---------------------------------------------------------------- plumbing

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.createDirectories(directory.resolve("state"));
        write(directory.resolve("witness.properties"),
                "bind.host=127.0.0.1\nbind.port=0\n"
                        + "state.directory=" + directory.resolve("state") + "\n"
                        + "secret.shared.value=" + SECRET + "\n");
        write(directory.resolve("applications/orders.properties"), "");
        write(directory.resolve("applications/billing.properties"), "");
    }

    @AfterEach
    void stopServing() {
        if (lifecycle != null) {
            lifecycle.stop();
        }
        if (server != null) {
            server.close();
        }
    }

    /**
     * Builds the server this module would build and runs it on the lifecycle this module would use,
     * so the health indicator is reading a real one.
     */
    private void serve(StateStoreFactory stateStores) {
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .stateStores(stateStores)
                .build();
        lifecycle = new WitnessServerLifecycle(server, Duration.ofSeconds(5));
        lifecycle.start();
    }

    private Health health() {
        return new WitnessHealthIndicator(server, lifecycle).health();
    }

    /** The failure happens on the server's own thread, so the report of it arrives when it does. */
    private Health awaitQuarantineOf(String application) {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        Health health = health();
        while (System.nanoTime() < deadline
                && !server.quarantinedApplications().contains(application)) {
            Thread.onSpinWait();
            health = health();
        }
        return health;
    }

    private WitnessTestClient client(String application) {
        return new WitnessTestClient(application, 1, SECRET, WITNESS_ID, server.boundAddress());
    }

    private void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Durable by its own account and unable to write: a disk that has gone bad. */
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
        }
    }

    /**
     * What the unaffected tenant runs on. {@code MemoryStateStore} reports itself non-durable and
     * the server refuses it, quite rightly; this is that lie told deliberately, in a test about
     * what happens to the tenants beside a broken one.
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
        }
    }
}
