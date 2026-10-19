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
package org.lolaf.primos.spring;

import org.lolaf.primos.Leader;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PeerDownReason;
import org.lolaf.primos.Role;

import java.util.Optional;

/**
 * The Spring events primos publishes, so an application can {@code @EventListener} rather than
 * implement {@link org.lolaf.primos.PrimosListener}.
 *
 * <p>Every event carries its own term. Use the value you are handed rather than re-reading the
 * node: dispatch happens off the node thread, so by the time a listener runs the answer may have
 * moved on. That is not a race to be fixed — it is why the term is on the event.
 */
public final class PrimosEvents {

    private PrimosEvents() {
    }

    /**
     * This node's role changed.
     *
     * @param previous what it was
     * @param current  what it now is
     * @param term     the term the change happened in
     */
    public record RoleChanged(Role previous, Role current, long term) {

        /**
         * Whether this node is now the leader.
         */
        public boolean isLeader() {
            return current == Role.LEADER;
        }
    }

    /**
     * Who the cluster's leader is, as far as this node knows.
     *
     * @param leader the leader, or empty when none is currently known — which is a real and common
     *               state during an election, not an error
     * @param term   the term this belief belongs to
     */
    public record LeaderChanged(Optional<Leader> leader, long term) {
    }

    /**
     * The leader published a change to its data with {@code PrimosNode.publishChange()}. Fetch the
     * data from it over your own channel and apply it only if it carries this term; when
     * {@link Leader#isSelf()}, this node is the leader and there is nothing to fetch.
     *
     * @param leader  the leader that published
     * @param term    its term, which the fetched data must carry
     * @param version the version published, the latest this node has heard of
     */
    public record LeaderVersion(Leader leader, long term, long version) {
    }

    /**
     * A configured peer became reachable.
     *
     * <p>Reachability, never membership: the cluster did not grow, and quorum is unchanged.
     *
     * @param peer the peer
     */
    public record PeerUp(Peer peer) {
    }

    /**
     * A configured peer stopped answering, or announced that it was leaving.
     *
     * <p><strong>This does not mean the cluster shrank.</strong> Quorum is computed from configured
     * membership and never from who happens to be reachable: five voters with three gone still need
     * three votes. Reading this event as "the cluster is smaller now" is how a split brain is talked
     * into existence.
     *
     * @param peer   the peer
     * @param reason {@code LEFT} when it said it was going, {@code UNREACHABLE} when it went silent and may come back
     */
    public record PeerDown(Peer peer, PeerDownReason reason) {
    }
}
