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
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

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
@Getter
@Builder(builderClassName = "Builder", buildMethodName = "buildUnvalidated")
public class ClusterConfig {

    /**
     * The application name travels in a fixed 32-byte field on every datagram, so it is bounded
     * here rather than truncated later: a name truncated to a colliding prefix would defeat the
     * point of having it.
     */
    public static final int APPLICATION_NAME_MAX_BYTES = 32;

    private final String applicationName;
    private final int nodeId;

    /**
     * Set through {@link Builder#asWitness()} rather than by naming the type, so that declaring a
     * witness is one named decision.
     */
    @lombok.Builder.Default
    private final PeerType nodeType = PeerType.APPLICATION;

    /**
     * The other application instances. This node must not be among them.
     *
     * <p><strong>Leaving this empty is a one-node cluster, and a one-node cluster leads
     * itself.</strong> Quorum is one, so the node elects itself about an election timeout after
     * starting and stays leader for as long as it runs — nothing can depose it, because there is no
     * majority for it to lose contact with. That is deliberate and supported: it makes development
     * trivial and lets an application run the same code path in a non-clustered deployment instead
     * of branching around primos entirely.
     *
     * <p>It is also, unavoidably, what a peer list that failed to load looks like. Made on every
     * node of an intended cluster, that mistake produces one permanent leader per node, each
     * reporting perfect health. {@link Quorum#warnings()} says so at startup for exactly this
     * reason.
     */
    @Singular
    private final List<Peer> peers;

    @Getter(AccessLevel.NONE)
    private final Peer witness;

    @Getter(AccessLevel.NONE)
    private final boolean learnedMembership;

    @Getter(AccessLevel.NONE)
    private final boolean followsStatedTimings;

    /**
     * Election, heartbeat, probe and advertisement intervals. Keep them uniform across a cluster,
     * witness included.
     */
    @lombok.Builder.Default
    private final Timings timings = Timings.lan();

    /**
     * Whether who voted for whom is logged at INFO rather than DEBUG. On by default.
     *
     * <p>An election is the one moment a cluster makes a collective decision, and afterwards the
     * only evidence left is which node ended up leading. Who asked, who agreed, who refused and why
     * is what turns "the failover took eight seconds" into an answer, and none of it is
     * reconstructable later — so it is on by default and costs nothing while a leader is healthy,
     * because a cluster that is not electing says nothing at all.
     *
     * <p>Turn it off for a deployment that elects often enough for the lines to be noise, or one
     * whose log ingestion is charged by the line. The decisions are still logged at DEBUG.
     */
    @lombok.Builder.Default
    boolean voteLogging = true;

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
    /**
     * Whether an owner outside this node applies what peers state, so a difference is not a fault.
     *
     * <p>See {@link ClusterConfig.Builder#followsStatedTimings()}. It suppresses the mismatch
     * report and nothing else; no decision anywhere reads it.
     */
    public boolean isFollowingStatedTimings() {
        return followsStatedTimings;
    }

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
     * Every rule, checked before a {@link ClusterConfig} escapes its builder.
     *
     * <p>Called from {@link Builder#build()} rather than from a constructor, because the
     * constructor is Lombok's now — which is why the generated build method is named
     * {@code buildUnvalidated} and wrapped.
     */
    private ClusterConfig validate() {
        Objects.requireNonNull(nodeType, "nodeType");
        Objects.requireNonNull(timings, "timings");
        Peer.requireValidId(nodeId, "nodeId");

        Set<Integer> seen = new HashSet<>();
        seen.add(nodeId);

        for (Peer peer : peers) {
            if (peer.type() != PeerType.APPLICATION) {
                throw new IllegalArgumentException(
                        "peer " + peer.id() + " is a " + peer.type() + "; configure a witness with witness(...) rather than as a peer");
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
        if (followsStatedTimings && !nodeType.isWitness()) {
            throw new IllegalArgumentException(
                    "only a witness may follow the intervals its peers state: an application node"
                            + " that stopped reporting a mismatch would lose the one signal that"
                            + " its cluster's profile has drifted");
        }
        if (peers.isEmpty() && nodeType.isWitness() && !learnedMembership) {
            throw new IllegalArgumentException(
                    "a witness with no peers has nothing to witness; a witness server whose members"
                            + " arrive rather than being configured wants learnedMembership()");
        }
        if (witness != null) {
            if (witness.type() != PeerType.WITNESS) {
                throw new IllegalArgumentException(
                        "the configured witness has type " + witness.type() + "; build it with Peer.witness(...)");
            }
            requireDistinctId(seen, witness.id());
        }
        return this;
    }

    private void requireDistinctId(Set<Integer> seen, int id) {
        if (!seen.add(id)) {
            throw new IllegalArgumentException("duplicate node id " + id + ": ids must be unique within an application,"
                    + " and this node's own id may not be repeated among its peers");
        }
    }

    public static class Builder {

        private static String requireAndTrimApplicationName(String name) {
            Objects.requireNonNull(name, "applicationName");
            String trimmed = getTrimmed(name);
            int bytes = trimmed.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > APPLICATION_NAME_MAX_BYTES) {
                throw new IllegalArgumentException(
                        "applicationName must be at most " + APPLICATION_NAME_MAX_BYTES + " bytes, but '" + trimmed + "' is " + bytes);
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
         * Declares this node to be the witness rather than an application instance: it votes, but
         * never campaigns and never leads. Its peers are the application nodes.
         *
         * <p>A named decision rather than {@code nodeType(WITNESS)}, which the generated setter
         * also allows — this is the one that says what it means.
         */
        public Builder asWitness() {
            return nodeType(PeerType.WITNESS);
        }

        /**
         * Declares that this witness's voters are not configured but learned from traffic.
         *
         * <p>For the standalone witness server, and only for it. That server holds no addresses in
         * configuration — only application names and secrets — because a witness that learns the
         * applications it serves cannot know their nodes ahead of time, and even when it is told
         * them, listing every node of every tenant on shared infrastructure is a burden nobody
         * would accept. So it
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
            // Set directly rather than through a generated setter: Lombok skips generating one when
            // a method of that name already exists, and this no-argument form is the name.
            this.learnedMembership = true;
            return this;
        }

        /**
         * Declares that something outside this node applies the intervals its peers state.
         *
         * <p>For a witness server told to follow the clusters it serves, and only for it. It
         * changes nothing this node does — the node still acts on no advertisement, and the value
         * it runs is still the one it was built with. What it changes is what gets <em>said</em>:
         * a difference between this node's intervals and a peer's is normally an error worth
         * shouting about, because somebody's failover is slower than they believe and nothing else
         * will ever mention it. Here it is not a disagreement at all, it is the value this node is
         * about to be rebuilt with, and reporting it would describe a state that lasts one turn
         * and reads as a fault every time a tenant retunes.
         *
         * <p>Only a witness may declare it, for the same reason only a witness may have learned
         * membership: an application node that stopped reporting mismatches would lose the one
         * signal that a cluster's profile has drifted.
         */
        public Builder followsStatedTimings() {
            // Set directly rather than through a generated setter, as learnedMembership does and
            // for the same reason: Lombok skips the setter when the name is already taken.
            this.followsStatedTimings = true;
            return this;
        }

        public ClusterConfig build() {
            this.applicationName = requireAndTrimApplicationName(this.applicationName);
            return buildUnvalidated().validate();
        }
    }
}
