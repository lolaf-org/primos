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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.IOException;
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
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.store.StateStoreException;
import org.lolaf.primos.time.MonotonicClock;

/**
 * A socket that never empties. The store factory runs inside the drain, so each call puts the next datagram on the
 * socket, which floods deterministically where a sending thread might lose the race and prove nothing.
 */
class DrainBudgetTest {

    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final String SECRET = "a-shared-cluster-secret";

    @TempDir
    Path directory;

    private WitnessServer server;
    private WitnessClient flooder;
    private int floodLimit = Integer.MAX_VALUE;
    private int routed;

    @BeforeEach
    void start() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        Files.write(directory.resolve("witness.properties"),
                ("config.source=cluster\nsecret.shared.value=" + SECRET).getBytes(StandardCharsets.UTF_8));
        Files.write(directory.resolve("applications/billing.properties"), new byte[0]);
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .stateStores(this::open)
                .build();
        flooder = new WitnessClient("flood", 2, SECRET, WITNESS_ID, server.boundAddress());
    }

    @AfterEach
    void close() {
        flooder.close();
        server.close();
    }

    @Test
    void a_socket_that_never_empties_still_ends_the_turn() {
        startFlood();

        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> server.runOnce(0L));

        assertThat(routed).isEqualTo(WitnessServer.MAX_DATAGRAMS_PER_TURN);
    }

    @Test
    void what_one_turn_leaves_behind_the_next_reads() {
        floodLimit = WitnessServer.MAX_DATAGRAMS_PER_TURN + 10;
        startFlood();

        for (int turns = 0; turns < 200 && routed < floodLimit; turns++) {
            server.runOnce(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(20));
        }

        assertThat(routed).isEqualTo(floodLimit);
    }

    @Test
    void a_healthy_tenant_still_answers_during_a_flood() {
        startFlood();

        try (WitnessClient voter = new WitnessClient("billing", 1, SECRET, WITNESS_ID, server.boundAddress())) {
            Boolean granted = assertTimeoutPreemptively(Duration.ofSeconds(20), () -> voter.requestVote(server, 1L));
            assertThat(granted).isTrue();
        }
    }

    private void startFlood() {
        flooder.sendConfig();
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private StateStore open(String application) {
        if ("flood".equals(application)) {
            routed++;
            if (routed < floodLimit) {
                flooder.sendConfig();
            }
            throw new StateStoreException("never opens, so the name stays unserved and the next datagram comes back here");
        }
        return new DurableMemoryStore();
    }

    private static final class DurableMemoryStore implements StateStore {

        private final MemoryStateStore memory = new MemoryStateStore();

        @Override
        public PersistentState load() {
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
            memory.close();
        }
    }
}
