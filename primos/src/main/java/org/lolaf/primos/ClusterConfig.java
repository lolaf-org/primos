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
package org.lolaf.primos;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The shape of one cluster: who is in it, which member this node is, and how fast it ticks.
 *
 * <p>Membership is static. Peers are never added or removed at runtime, and quorum is always
 * computed from the configured voter set rather than from who happens to be reachable, so a
 * cluster of five with two down still needs three votes. Reading an unreachable peer as "the
 * cluster shrank" is how a split brain gets talked into existence.
 */
@Value
public class ClusterConfig {

    /**
     * The application name travels in a fixed 32-byte field on every datagram, so it is bounded
     * here rather than truncated later: a name truncated to a colliding prefix would defeat the
     * point of having it.
     */
    public static final int APPLICATION_NAME_MAX_BYTES = 32;

    String applicationName;
    int nodeId;
    String nodeName;
    PeerType nodeType;
    List<Peer> peers;

    @Getter(AccessLevel.NONE)
    Peer witness;

    @Getter(AccessLevel.NONE)
    boolean learnedMembership;

    Timings timings;

    private ClusterConfig(Builder builder) {
        this.applicationName = requireApplicationName(builder.applicationName);
        this.nodeId = Peer.requireValidId(builder.nodeId, "nodeId");
        this.nodeType = Objects.requireNonNull(builder.nodeType, "nodeType");
        this.nodeName = (builder.nodeName == null || builder.nodeName.trim().isEmpty())
                ? (this.nodeType.isWitness() ? "witness-" : "node-") + this.nodeId
                : builder.nodeName.trim();
        this.peers = Collections.unmodifiableList(new ArrayList<>(builder.peers));
        this.witness = builder.witness;
        this.learnedMembership = builder.learnedMembership;
        this.timings = Objects.requireNonNull(builder.timings, "timings");
        validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Whether a character may appear in an application name: printable ASCII with no space, which
     * is {@code '!'} through {@code '~'}.
     *
     * <p>Public because three places have to agree on it — names are validated here, read back off
     * the wire by the decoder, and taken from filenames by the witness's config store — and a name
     * one of them accepted and another refused would be a cluster that can be configured and not
     * reached.
     *
     * <p>Both bounds earn their place. Below {@code '!'} lie the space and the control characters:
     * a space would make {@code "orders "} and {@code "orders"} two different clusters that look
     * identical in a log line, and a control character would put escape sequences into every log
     * line that names the cluster, since the name is printed as it arrived. Above {@code '~'} is
     * everything non-ASCII, which the fixed 32-byte wire field measures in bytes rather than
     * characters, so a name that looked comfortably short could still fail to fit.
     */
    public static boolean isApplicationNameCharacter(char c) {
        return c >= '!' && c <= '~';
    }

    private static String requireApplicationName(String name) {
        Objects.requireNonNull(name, "applicationName");
        String trimmed = getTrimmed(name);
        int bytes = trimmed.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > APPLICATION_NAME_MAX_BYTES) {
            throw new IllegalArgumentException(
                    "applicationName must be at most " + APPLICATION_NAME_MAX_BYTES
                            + " bytes, but '" + trimmed + "' is " + bytes);
        }
        return trimmed;
    }

    private static String getTrimmed(String name) {
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("applicationName must not be blank");
        }
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (!isApplicationNameCharacter(c)) {
                throw new IllegalArgumentException(
                        "applicationName must be printable ASCII without spaces, but '" + trimmed
                                + "' contains a character at index " + i + " that is not");
            }
        }
        return trimmed;
    }

    /**
     * The witness, if this cluster has one. Most clusters do not.
     */
    public Optional<Peer> witness() {
        return Optional.ofNullable(witness);
    }

    /**
     * Whether the cluster has a witness — either configured as a peer, or this node being it.
     */
    public boolean hasWitness() {
        return witness != null || nodeType.isWitness();
    }

    /**
     * Whether this node is itself the witness, and so must never campaign.
     */
    public boolean isWitness() {
        return nodeType.isWitness();
    }

    /**
     * Whether this witness's voters arrive rather than being configured.
     *
     * <p>True only for the standalone witness server. See
     * {@link ClusterConfig.Builder#learnedMembership()}.
     */
    public boolean isLearnedMembership() {
        return learnedMembership;
    }

    /**
     * Every peer this node exchanges messages with, the witness included. Excludes this node.
     */
    public List<Peer> allPeers() {
        if (witness == null) {
            return peers;
        }
        List<Peer> all = new ArrayList<>(peers.size() + 1);
        all.addAll(peers);
        all.add(witness);
        return Collections.unmodifiableList(all);
    }

    /**
     * How many instances of the application are in the cluster, this node included and the
     * witness excluded. The witness does not run the application, so it is not one of them.
     */
    public int applicationNodeCount() {
        return peers.size() + (nodeType.isWitness() ? 0 : 1);
    }

    /**
     * How many voters the cluster has: the application nodes plus the witness, if any.
     */
    public int voterCount() {
        return applicationNodeCount() + (hasWitness() ? 1 : 0);
    }

    private void validate() {
        Set<Integer> seen = new HashSet<>();
        seen.add(nodeId);

        for (Peer peer : peers) {
            if (peer.type() != PeerType.APPLICATION) {
                throw new IllegalArgumentException(
                        "peer " + peer.id() + " is a " + peer.type()
                                + "; configure a witness with witness(...) rather than as a peer");
            }
            requireDistinctId(seen, peer.id());
        }
        if (witness != null && nodeType.isWitness()) {
            throw new IllegalArgumentException(
                    "this node is the witness, so it cannot also have one: a cluster has at most"
                            + " one, and two would give a pair four voters needing three");
        }
        if (learnedMembership && !nodeType.isWitness()) {
            throw new IllegalArgumentException(
                    "only a witness may have learned membership: an application node is configured"
                            + " with its peers, and accepting whoever turns up would let anyone who"
                            + " can reach the port join the vote");
        }
        if (peers.isEmpty() && nodeType.isWitness() && !learnedMembership) {
            throw new IllegalArgumentException(
                    "a witness with no peers has nothing to witness; a witness server whose members"
                            + " arrive rather than being configured wants learnedMembership()");
        }
        if (witness != null) {
            if (witness.type() != PeerType.WITNESS) {
                throw new IllegalArgumentException(
                        "the configured witness has type " + witness.type()
                                + "; build it with Peer.witness(...)");
            }
            requireDistinctId(seen, witness.id());
        }
    }

    private void requireDistinctId(Set<Integer> seen, int id) {
        if (!seen.add(id)) {
            throw new IllegalArgumentException(
                    "duplicate node id " + id + ": ids must be unique within an application,"
                            + " and this node's own id may not be repeated among its peers");
        }
    }

    /**
     * Builder for {@link ClusterConfig}. Validation happens in {@link #build()}.
     */
    public static final class Builder {

        private final List<Peer> peers = new ArrayList<>();
        /**
         * Which clustered application this is. Every node of one cluster must agree on it.
         */
        @Setter
        private String applicationName;
        /**
         * This node's id, unique within the application.
         */
        @Setter
        private int nodeId;
        /**
         * A human-readable name for this node, used in logs. Defaults to {@code node-<id>}.
         */
        @Setter
        private String nodeName;
        /**
         * Set through {@link #asWitness()} rather than directly, so that declaring a witness is one
         * named decision rather than a type a caller has to know to pass.
         */
        private PeerType nodeType = PeerType.APPLICATION;
        /**
         * The witness, at most one. Two would give a two-node pair four voters needing three,
         * which is strictly worse than one.
         */
        @Setter
        private Peer witness;

        /**
         * Set through {@link #learnedMembership()}, which documents what it costs.
         */
        private boolean learnedMembership;

        /**
         * Election, heartbeat and probe intervals.
         */
        @Setter
        private Timings timings = Timings.lan();

        private Builder() {
        }

        /**
         * Declares this node to be the witness rather than an application instance: it votes, but
         * never campaigns and never leads. Its peers are the application nodes.
         */
        public Builder asWitness() {
            this.nodeType = PeerType.WITNESS;
            return this;
        }

        /**
         * The other application instances. This node must not be among them.
         */
        public Builder peers(Peer... peers) {
            return peers(java.util.Arrays.asList(peers));
        }

        /**
         * The other application instances. This node must not be among them.
         */
        public Builder peers(List<Peer> peers) {
            this.peers.clear();
            this.peers.addAll(Objects.requireNonNull(peers, "peers"));
            return this;
        }

        /**
         * Declares that this witness's voters are not configured but learned from traffic.
         *
         * <p>For the standalone witness server, and only for it. That server holds no addresses in
         * configuration — only application names and secrets — because in permissive mode the
         * clusters it serves are not known ahead of time, and even in strict mode listing every
         * node of every tenant on shared infrastructure is a burden nobody would accept. So it
         * learns each node's endpoint from the first datagram that passes authentication, and
         * probes it thereafter.
         *
         * <p><strong>The authentication tag is the whole of the admission control here.</strong>
         * With a configured peer set an unknown sender id is dropped outright; with a learned one
         * it is not, so a witness serving an application without a secret will answer anybody who
         * asks. That is exactly the case where a forged reply manufactures a leader, which is why
         * an unauthenticated witness link is never a documented option.
         *
         * <p>Only a witness may do this, and a witness that does may start with no peers at all.
         */
        public Builder learnedMembership() {
            this.learnedMembership = true;
            return this;
        }

        public ClusterConfig build() {
            return new ClusterConfig(this);
        }
    }
}
