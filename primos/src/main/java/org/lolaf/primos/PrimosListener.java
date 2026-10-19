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

import java.util.Optional;

/**
 * Receives everything that happens to a node: its own role changing, the leader changing, and
 * peers coming and going.
 *
 * <p>Every method has a default implementation, so implement only what you care about.
 *
 * <p><strong>Callbacks run on the node thread.</strong> A listener that blocks — a slow log write,
 * a database call, a contended lock — stalls consensus, misses heartbeats and can trigger a
 * spurious election. Do only trivial work here and hand anything real to your own executor. If
 * that is inconvenient, supply an executor when building the node and dispatch moves off the node
 * thread entirely; a watchdog warns either way when a callback runs long.
 *
 * <p>Events arrive in order, one at a time, and an exception thrown from one is logged and
 * swallowed rather than allowed to kill the node. Each event carries the term and role it belongs
 * to: use the values handed to you rather than re-reading {@link PrimosNode#role()}, which may
 * already have moved on.
 */
public interface PrimosListener {

    /**
     * This node's role changed. The term is the one the new role is held in, and it is the fencing
     * token: guarding external work on the role alone is unsafe, on the term it is not.
     */
    default void onRoleChanged(Role previous, Role current, long term) {
    }

    /**
     * The cluster's leader changed, which is not the same event as this node's role changing — a
     * follower needs this to know where to forward work. Empty means no leader is currently known,
     * which is a real and common state during an election, not an error.
     *
     * <p>A {@link Leader} rather than a bare id, so that a listener told where the leader is does
     * not then have to look it up. {@link Leader#peer()} is the address to forward to; it is empty
     * when {@link Leader#isSelf()}, since there is nowhere to forward to when the work is here.
     */
    default void onLeaderChanged(Optional<Leader> leader, long term) {
    }

    /**
     * The leader published a change with {@link PrimosNode#publishChange()}: fetch its data from it,
     * over your own channel, and apply it only if it carries this term. Told once per publication,
     * never for version 0, and to the leader too, where {@link Leader#isSelf()} says there is
     * nothing to fetch.
     */
    default void onLeaderVersion(Leader leader, long term, long version) {
    }

    /**
     * A peer became reachable. Check {@link Peer#isWitness()} before treating it as somewhere work
     * can be sent: a witness votes but does not run the application.
     */
    default void onPeerUp(Peer peer) {
    }

    /**
     * A peer is no longer reachable, either because it said it was leaving or because it stopped
     * answering probes; the reason says which. This says nothing about quorum, which is always
     * computed from the configured voter set: a peer being down does not shrink the cluster.
     *
     * <p>A witness going down deserves its own alert. Nothing visibly breaks, but the cluster has
     * quietly lost its ability to survive a partition.
     */
    default void onPeerDown(Peer peer, PeerDownReason reason) {
    }
}
