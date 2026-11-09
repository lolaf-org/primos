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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.Peer;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.store.StateStoreException;
import org.lolaf.primos.time.MonotonicClock;

/**
 * One tenant's storage failing while its node is being created or rebuilt costs that tenant and nobody else.
 */
class TenantIsolationTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";
    private static final String ORDERS = "orders";
    private static final String BILLING = "billing";

    @TempDir
    Path directory;

    private final Set<String> corrupt = ConcurrentHashMap.newKeySet();
    private final Map<String, ControllableStore> stores = new ConcurrentHashMap<>();
    private int loadsBeforeCorrupt = Integer.MAX_VALUE;
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

    @Test
    void a_tenant_that_cannot_auto_register_costs_nobody_else() {
        cluster();
        corrupt.add(ORDERS);
        start();

        announce(ORDERS);

        assertThat(server.servedApplications()).doesNotContain(ORDERS);
        assertThat(server.quarantinedApplications()).containsExactly(ORDERS);
        assertThat(stores.get(ORDERS).closed).as("its lock must be free for the retry").isTrue();
        announce(BILLING);
        assertThat(voteFor(BILLING, SECRET, 1L)).isTrue();
    }

    @Test
    void a_repaired_tenant_is_served_on_its_next_announcement() {
        cluster();
        corrupt.add(ORDERS);
        start();
        announce(ORDERS);

        corrupt.remove(ORDERS);
        announce(ORDERS);

        assertThat(server.servedApplications()).contains(ORDERS);
        assertThat(server.quarantinedApplications()).isEmpty();
        assertThat(voteFor(ORDERS, SECRET, 1L)).isTrue();
    }

    @Test
    void a_tenant_added_by_a_reload_that_cannot_open_costs_nobody_else() {
        settings("secret.shared.value = " + SECRET);
        application(BILLING, "");
        corrupt.add(ORDERS);
        start();

        application(ORDERS, "");
        reload();

        assertThat(server.quarantinedApplications()).containsExactly(ORDERS);
        assertThat(voteFor(BILLING, SECRET, 1L)).isTrue();
    }

    @Test
    void a_reload_after_the_repair_serves_the_tenant() {
        settings("secret.shared.value = " + SECRET);
        corrupt.add(ORDERS);
        start();
        application(ORDERS, "");
        reload();

        corrupt.remove(ORDERS);
        application(ORDERS, "timings.profile=wan");
        reload();

        assertThat(server.quarantinedApplications()).isEmpty();
        assertThat(voteFor(ORDERS, SECRET, 1L)).isTrue();
    }

    @Test
    void a_tenant_that_fails_its_rebuild_is_quarantined_and_keeps_no_old_key() {
        String billingKey = "secret.billing.value = billing-secret\nsecret.billing.applications = billing\n";
        settings(billingKey + "secret.orders.value = old-secret\nsecret.orders.applications = orders");
        application(ORDERS, "");
        application(BILLING, "");
        start();

        stores.get(ORDERS).corrupt = true;
        settings(billingKey + "secret.orders.value = new-secret\nsecret.orders.applications = orders");
        reload();

        assertThat(server.quarantinedApplications()).containsExactly(ORDERS);
        assertThat(voteFor(ORDERS, "old-secret", 1L))
                .as("a rotation that could not be applied must not leave the removed key working")
                .isNull();
        assertThat(server.routing().droppedForFailedApplication()).isPositive();
        assertThat(voteFor(BILLING, "billing-secret", 1L)).isTrue();
    }

    @Test
    void a_tenant_that_fails_on_starting_is_not_left_registered() {
        cluster();
        loadsBeforeCorrupt = 1;
        start();

        announce(ORDERS);

        assertThat(server.servedApplications()).doesNotContain(ORDERS);
        assertThat(server.quarantinedApplications()).containsExactly(ORDERS);
    }

    @Test
    void a_quarantined_rebuild_stops_talking_to_its_cluster() {
        settings("secret.orders.value = old-secret\nsecret.orders.applications = orders");
        application(ORDERS, "");
        start();
        try (WitnessClient node = new WitnessClient(ORDERS, 1, "old-secret", WITNESS_ID, server.boundAddress())) {
            assertThat(node.requestVote(server, 1L)).as("so the witness knows where this node is").isTrue();
            assertThat(node.awaitNodeConfig(server)).as("and it advertises to it while healthy").isNotNull();

            stores.get(ORDERS).corrupt = true;
            settings("secret.orders.value = new-secret\nsecret.orders.applications = orders");
            reload();
            node.discardReceived();

            assertThat(node.awaitNodeConfig(server)).as("nothing is driving the abandoned node any more").isNull();
        }
    }

    @Test
    void a_quarantined_rebuild_does_not_spin_the_loop() {
        settings("secret.orders.value = old-secret\nsecret.orders.applications = orders");
        application(ORDERS, "");
        start();
        stores.get(ORDERS).corrupt = true;
        settings("secret.orders.value = new-secret\nsecret.orders.applications = orders");
        reload();

        int turns = 0;
        long deadline = System.nanoTime() + Duration.ofMillis(500).toNanos();
        while (System.nanoTime() < deadline) {
            server.runOnce(Duration.ofMillis(100).toNanos());
            turns++;
        }

        assertThat(turns).as("an abandoned node's expired deadlines must not wake the loop").isLessThan(100);
    }

    @Test
    void a_broken_tenant_removed_from_the_configuration_stops_being_listed() throws IOException {
        settings("secret.shared.value = " + SECRET);
        corrupt.add(ORDERS);
        start();
        application(ORDERS, "");
        reload();
        assertThat(server.quarantinedApplications()).containsExactly(ORDERS);

        Files.delete(directory.resolve("applications/orders.properties"));
        reload();

        assertThat(server.quarantinedApplications()).isEmpty();
    }

    @Test
    void a_healthy_tenant_is_never_listed() {
        settings("secret.shared.value = " + SECRET);
        application(ORDERS, "");
        start();

        assertThat(server.quarantinedApplications()).isEmpty();
        assertThat(voteFor(ORDERS, SECRET, 1L)).isTrue();
    }

    private void cluster() {
        settings("config.source=cluster\nsecret.shared.value = " + SECRET);
    }

    private void start() {
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .stateStores(name -> {
                    ControllableStore store = new ControllableStore();
                    store.corrupt = corrupt.contains(name);
                    store.loadsLeft = loadsBeforeCorrupt;
                    stores.put(name, store);
                    return store;
                })
                .build();
    }

    private void announce(String application) {
        try (WitnessClient node = new WitnessClient(application, 3, SECRET, WITNESS_ID, server.boundAddress())) {
            node.announceConfig(server);
        }
    }

    private Boolean voteFor(String application, String secret, long term) {
        try (WitnessClient node = new WitnessClient(application, 1, secret, WITNESS_ID, server.boundAddress())) {
            return node.requestVote(server, term);
        }
    }

    private void reload() {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            server.runOnce(Duration.ofMillis(20).toNanos());
        }
    }

    private void settings(String contents) {
        write(directory.resolve("witness.properties"), contents);
    }

    private void application(String name, String contents) {
        write(directory.resolve("applications/" + name + ".properties"), contents);
    }

    private static void write(Path file, String contents) {
        try {
            Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static final class ControllableStore implements StateStore {

        private final MemoryStateStore memory = new MemoryStateStore();
        private volatile boolean corrupt;
        private volatile boolean closed;
        private int loadsLeft;

        @Override
        public PersistentState load() {
            if (corrupt || loadsLeft-- <= 0) {
                throw new StateStoreException("this state file is corrupt");
            }
            return memory.load();
        }

        @Override
        public void save(long currentTerm, int votedFor) {
            memory.save(currentTerm, votedFor);
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
