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

/**
 * The role this node currently holds in its cluster.
 *
 * <p>Read it with {@link PrimosNode#role()}, which is a cheap volatile read safe to call from an
 * application hot path. Note that {@code role() == LEADER} alone is <em>not</em> a safe guard for
 * an external side effect: leadership can be lost at any instant and the news travels no faster
 * than the network, so there is always a window in which this node believes it leads and does not.
 * Use {@link PrimosNode#roleAndTerm()} and carry the term into whatever system the work touches,
 * so that system can reject a stale leader.
 */
public enum Role {

    /**
     * Not leading. Accepts heartbeats and votes; campaigns when its election timeout expires.
     */
    FOLLOWER,

    /**
     * Campaigning for leadership in a term it has already incremented and persisted.
     */
    CANDIDATE,

    /**
     * Leading: sending heartbeats, and stepping down if it stops hearing from a majority.
     */
    LEADER,

    /**
     * Terminal. The node thread has died and the node will never lead again.
     *
     * <p>It exists so that an uncaught failure cannot leave a node reporting {@code LEADER}
     * forever while the rest of the cluster has moved on. Treat it as a signal to restart the
     * process; a failed node cannot be restarted in place.
     */
    FAILED;

    /**
     * Whether this role is {@link #LEADER}. Never true for {@link #FAILED}.
     */
    public boolean isLeader() {
        return this == LEADER;
    }
}
