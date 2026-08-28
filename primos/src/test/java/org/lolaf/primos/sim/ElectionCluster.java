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
package org.lolaf.primos.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.Role;
import org.lolaf.primos.Timings;
import org.lolaf.primos.core.ElectionNode;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

/**
 * Builds a cluster of real {@link ElectionNode}s on the simulated network.
 *
 * <p>The nodes here are the production ones, not stand-ins. The only things replaced are the clock,
 * the network and the disk, which is the whole point: the algorithm under test is the algorithm
 * that ships.
 */
public final class ElectionCluster {

    static final int WITNESS_ID = 99;

    private final Simulation simulation;
    private final List<NodeUnderTest> nodes = new ArrayList<>();

    private ElectionCluster(Simulation simulation) {
        this.simulation = simulation;
    }

    /** {@code applicationNodes} instances, optionally with a witness alongside them. */
    public static ElectionCluster of(Simulation simulation, int applicationNodes, boolean withWitness) {
        return of(simulation, applicationNodes, withWitness, Timings.lan());
    }

    public static ElectionCluster of(Simulation simulation, int applicationNodes,
                                     boolean withWitness, Timings timings) {
        ElectionCluster cluster = new ElectionCluster(simulation);
        Peer witnessPeer = withWitness ? Peer.witness(WITNESS_ID, "sim", 7000) : null;

        for (int id = 1; id <= applicationNodes; id++) {
            ClusterConfig.Builder config = ClusterConfig.builder()
                    .applicationName("sim")
                    .nodeId(id)
                    .timings(timings)
                    .peers(applicationPeersExcept(id, applicationNodes));
            if (withWitness) {
                config.witness(witnessPeer);
            }
            cluster.add(config.build());
        }
        if (withWitness) {
            cluster.add(ClusterConfig.builder()
                    .applicationName("sim")
                    .nodeId(WITNESS_ID)
                    .timings(timings)
                    .asWitness()
                    .peers(applicationPeersExcept(-1, applicationNodes))
                    .build());
        }
        return cluster;
    }

    private static List<Peer> applicationPeersExcept(int self, int applicationNodes) {
        List<Peer> peers = new ArrayList<>();
        for (int id = 1; id <= applicationNodes; id++) {
            if (id != self) {
                peers.add(Peer.application(id, "sim", 7000 + id));
            }
        }
        return peers;
    }

    private void add(ClusterConfig config) {
        NodeUnderTest node = new NodeUnderTest(simulation, config);
        nodes.add(node);
        simulation.add(node);
        node.restart();
    }

    /** Every node, witness included. */
    public List<NodeUnderTest> nodes() {
        return nodes;
    }

    public NodeUnderTest node(int id) {
        for (NodeUnderTest node : nodes) {
            if (node.id() == id) {
                return node;
            }
        }
        throw new IllegalArgumentException("no node " + id);
    }

    /** How many running nodes currently believe they are leader. */
    public int leaderCount() {
        int leaders = 0;
        for (NodeUnderTest node : nodes) {
            if (node.isRunning() && node.role() == Role.LEADER) {
                leaders++;
            }
        }
        return leaders;
    }

    /** The single leader, if there is exactly one. */
    public Optional<NodeUnderTest> leader() {
        NodeUnderTest found = null;
        for (NodeUnderTest node : nodes) {
            if (node.isRunning() && node.role() == Role.LEADER) {
                if (found != null) {
                    return Optional.empty();
                }
                found = node;
            }
        }
        return Optional.ofNullable(found);
    }

    /** Whether exactly one node currently leads. */
    public boolean hasSingleLeader() {
        return leaderCount() == 1;
    }

    /** A node that owns a real {@link ElectionNode}, and can be killed and brought back. */
    public static final class NodeUnderTest implements SimNode {

        private final Simulation simulation;
        private final ClusterConfig config;
        private final StateStore store = new MemoryStateStore();
        private final Transport transport;

        private ElectionNode node;
        private boolean running;
        private PrimosListener listener = new PrimosListener() {
        };

        private NodeUnderTest(Simulation simulation, ClusterConfig config) {
            this.simulation = simulation;
            this.config = config;
            this.transport = simulation.network().transportFor(config.nodeId());
        }

        /** Watches this node's events. Survives a crash and restart, as a real listener would. */
        public void attach(PrimosListener listener) {
            this.listener = listener;
            restart();
        }

        /** The live node, for tests that need to reach past the harness. */
        public ElectionNode election() {
            return node;
        }

        public ClusterConfig config() {
            return config;
        }

        public StateStore store() {
            return store;
        }

        @Override
        public int id() {
            return config.nodeId();
        }

        @Override
        public boolean isWitness() {
            return config.isWitness();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public long nextDeadlineNanos() {
            return running ? node.nextDeadlineNanos() : Long.MAX_VALUE;
        }

        @Override
        public void tick() {
            node.runOnce(0L);
        }

        @Override
        public Role role() {
            return running ? node.role() : Role.FOLLOWER;
        }

        @Override
        public long currentTerm() {
            return running ? node.currentTerm() : -1;
        }

        @Override
        public int votedFor() {
            return running ? node.votedFor() : -1;
        }

        @Override
        public void crash() {
            running = false;
            node = null;
        }

        @Override
        public void restart() {
            node = new ElectionNode(config, store, transport, simulation.clock(),
                    new Random(simulation.seed() * 31 + config.nodeId()),
                    MessageAuthenticator.disabled(), listener);
            node.start();
            running = true;
        }

        @Override
        public String toString() {
            return "n" + id() + (isWitness() ? "(w)" : "") + " " + role() + "/" + currentTerm();
        }
    }
}
