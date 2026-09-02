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
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.UdpTransport;

/**
 * Three two-node clusters, one witness, and every pair partitioned at once.
 *
 * <p>This is the deployment the witness exists for, multiplied. Each pair is an active/passive
 * couple that on its own tolerates zero failures; each is split from itself and can reach only the
 * witness; and all three are asking the same process, at the same time, on the same socket.
 *
 * <p>What it is really testing is that the buckets are independent. Each cluster has the same two
 * node ids and is racing for a vote in the same terms, so a witness that leaked state between
 * applications would grant one cluster's vote to another's candidate and produce exactly the split
 * brain it was deployed to prevent. The deterministic simulation proves the algorithm; this proves
 * that one process running three of them keeps them apart.
 */
class MultiApplicationSoakTest {

    private static final String LOOPBACK = "127.0.0.1";
    private static final String SECRET = "a-shared-cluster-secret";
    private static final int WITNESS_ID = Peer.MAX_ID;
    private static final List<String> APPLICATIONS = Arrays.asList("orders", "billing", "shipping");

    /** Fast enough to finish, slow enough that a loaded CI machine is not the thing under test. */
    private static final Timings FAST = Timings.builder()
            .heartbeatInterval(Duration.ofMillis(25))
            .electionTimeoutMin(Duration.ofMillis(150))
            .electionTimeoutMax(Duration.ofMillis(300))
            .probeInterval(Duration.ofMillis(100))
            .build();

    @TempDir
    Path directory;

    private final List<PrimosNode> nodes = new ArrayList<>();
    private final List<DatagramChannel> blackHoles = new ArrayList<>();
    private WitnessServer server;
    private Thread serverThread;

    @BeforeEach
    void createLayout() throws IOException {
        Files.createDirectories(directory.resolve("applications"));
        for (String application : APPLICATIONS) {
            Files.write(directory.resolve("applications/" + application + ".properties"),
                    ("secret=" + SECRET).getBytes(StandardCharsets.UTF_8));
        }
    }

    @AfterEach
    void stopEverything() {
        nodes.forEach(PrimosNode::stop);
        stopServer();
        for (DatagramChannel blackHole : blackHoles) {
            try {
                blackHole.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void three_partitioned_pairs_each_end_with_one_leader_and_their_own_vote() {
        startWitness();
        Map<String, List<PrimosNode>> clusters = new LinkedHashMap<>();
        for (String application : APPLICATIONS) {
            clusters.put(application, startPartitionedPair(application));
        }

        for (Map.Entry<String, List<PrimosNode>> cluster : clusters.entrySet()) {
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> leaderCount(cluster.getValue()) == 1);
        }

        Map<String, Integer> winners = new LinkedHashMap<>();
        for (Map.Entry<String, List<PrimosNode>> cluster : clusters.entrySet()) {
            assertThat(leaderCount(cluster.getValue()))
                    .as("cluster '%s': the half that reached the witness leads and the other stands"
                            + " down; never both", cluster.getKey())
                    .isEqualTo(1);
            winners.put(cluster.getKey(), leaderOf(cluster.getValue()));
        }

        // Read after the loop has stopped, so nothing is read from under the server thread.
        stopServer();

        for (Map.Entry<String, Integer> winner : winners.entrySet()) {
            assertThat(server.votedFor(winner.getKey()))
                    .as("cluster '%s': the witness's own record names the node that actually leads,"
                            + " which is what makes the majority real rather than assumed",
                            winner.getKey())
                    .isEqualTo(winner.getValue());
        }
        assertThat(server.servedApplications()).containsExactlyInAnyOrderElementsOf(APPLICATIONS);
        assertThat(server.routing().unservedApplication())
                .as("every datagram belonged to a configured cluster")
                .isZero();
        assertThat(server.routing().undeliverableReplies())
                .as("every reply reached somebody, including the first from each node — those are"
                        + " answered before the sender has been learned, and getting that wrong"
                        + " costs every new node a whole election timeout to retry")
                .isZero();
        assertThat(server.routing().inboxOverflowed()).isZero();

        for (String application : APPLICATIONS) {
            assertThat(directory.resolve("state/" + application + "/state"))
                    .as("a file each, so one cluster's vote cannot be spent by another")
                    .exists();
        }
    }

    @Test
    void a_pairs_survivor_keeps_leading_when_its_partner_stops() {
        startWitness();
        List<PrimosNode> pair = startPartitionedPair("orders");
        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderCount(pair) == 1);

        int leader = leaderOf(pair);
        PrimosNode loser = pair.get(0).config().nodeId() == leader ? pair.get(1) : pair.get(0);
        loser.stop();

        // Nothing should move: the leader still has itself and the witness, which is two of three.
        long steady = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < steady) {
            assertThat(leaderCount(pair)).isEqualTo(1);
        }
        assertThat(leaderOf(pair)).isEqualTo(leader);
    }

    /**
     * The whole of {@code config.source=cluster}, end to end and over real sockets.
     *
     * <p>Nothing configures this cluster here: no file names it, and the witness is built without
     * the timings override the other scenarios use, so what it runs is what the nodes state. A pair
     * that fails over correctly is the proof, because every step had to work — the nodes' config
     * messages had to arrive and create the application, the witness had to adopt their intervals,
     * and it had to grant the vote that breaks the tie on them.
     */
    @Test
    void a_cluster_nobody_configured_is_served_on_the_intervals_it_states() throws IOException {
        Files.write(directory.resolve("witness.properties"),
                ("config.source=cluster\nsecret=" + SECRET).getBytes(StandardCharsets.UTF_8));
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .build();
        serverThread = new Thread(server::run, "witness-under-test");
        serverThread.setDaemon(true);
        serverThread.start();

        List<PrimosNode> pair = startPartitionedPair("payments");

        await().atMost(30, TimeUnit.SECONDS).until(() -> leaderCount(pair) == 1);
        assertThat(server.servedApplications()).contains("payments");
        assertThat(server.votedFor("payments"))
                .as("the witness's own record names the node that actually leads")
                .isEqualTo(leaderOf(pair));
    }

    // ---------------------------------------------------------------- plumbing

    private void startWitness() {
        server = WitnessServer.builder()
                .configStore(new FileConfigStore(directory, MonotonicClock.SYSTEM))
                .nodeId(WITNESS_ID)
                .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                .timings(FAST)
                .build();
        serverThread = new Thread(server::run, "witness-under-test");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    private void stopServer() {
        if (serverThread == null) {
            return;
        }
        server.stop();
        try {
            serverThread.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        serverThread = null;
    }

    /**
     * Two nodes that cannot see each other but can both see the witness.
     *
     * <p>The partition is made by pointing each node at a socket nobody reads rather than by
     * stopping anything: both halves are alive, both are campaigning, and neither hears the other,
     * which is the situation a two-node pair cannot survive without a third voter.
     */
    private List<PrimosNode> startPartitionedPair(String application) {
        int blackHolePort = blackHole();
        InetSocketAddress witness = server.boundAddress();
        List<PrimosNode> pair = new ArrayList<>(2);
        for (int nodeId : new int[] {1, 2}) {
            int other = nodeId == 1 ? 2 : 1;
            pair.add(start(application, nodeId,
                    Peer.application(other, LOOPBACK, blackHolePort),
                    Peer.witness(WITNESS_ID, LOOPBACK, witness.getPort())));
        }
        return pair;
    }

    private PrimosNode start(String application, int nodeId, Peer peer, Peer witness) {
        UdpTransport transport;
        try {
            transport = UdpTransport.builder()
                    .bindAddress(new InetSocketAddress(LOOPBACK, 0))
                    .peers(Arrays.asList(peer, witness))
                    .addressLearning(true)
                    .clusterName(application)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        PrimosNode node = PrimosNode.builder()
                .clusterConfig(ClusterConfig.builder()
                        .applicationName(application)
                        .nodeId(nodeId)
                        .peer(peer)
                        .witness(witness)
                        .timings(FAST)
                        .build())
                .store(new MemoryStateStore())
                .transport(transport)
                .clusterSecret(SECRET.getBytes(StandardCharsets.UTF_8))
                .build();
        nodes.add(node);
        node.start();
        return node;
    }

    /** A bound socket nobody ever reads, which is a partition without an ICMP rejection. */
    private int blackHole() {
        try {
            DatagramChannel channel = DatagramChannel.open();
            channel.bind(new InetSocketAddress(LOOPBACK, 0));
            blackHoles.add(channel);
            return ((InetSocketAddress) channel.getLocalAddress()).getPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long leaderCount(List<PrimosNode> cluster) {
        return cluster.stream().filter(node -> node.role() == Role.LEADER).count();
    }

    private static int leaderOf(List<PrimosNode> cluster) {
        return cluster.stream()
                .filter(node -> node.role() == Role.LEADER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("nobody is leading"))
                .config()
                .nodeId();
    }
}
