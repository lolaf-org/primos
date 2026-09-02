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
 * Told what a node did, as it does it. The whole observability seam.
 *
 * <p>primos depends on nothing but SLF4J, so it cannot export metrics itself. It tells you instead,
 * and you increment whatever you already run — {@code primos-micrometer} is exactly that, and this
 * interface is what it is written against.
 *
 * <p><strong>Push rather than a snapshot, and there are three reasons a poller cannot substitute.</strong>
 * A <em>distribution</em>: a p99 of probe round trips needs every sample, and no sampling interval
 * recovers one from a last-value gauge. A <em>duration between two samples</em>: how long a campaign
 * took, how long the cluster had no leader — the two numbers most wanted after a failover, and both
 * gone before anybody looks. And an <em>event already superseded</em>: leadership going A to B to A
 * inside one sampling interval is invisible to a poller and is exactly the flap worth alerting on.
 *
 * <h2>The contract, which is harder than {@link PrimosListener}'s</h2>
 *
 * <p><strong>Callbacks must not block, and must not allocate.</strong> They run on the node thread,
 * and some of them run per datagram — a listener that allocates turns the node's steady state from
 * zero bytes a turn into whatever it allocates, and one that blocks stalls consensus, misses
 * heartbeats and causes the election it was watching for. That is why every method here takes
 * primitives and enums and never an event object, why there is <em>one</em> sink fixed at build
 * time rather than a list to iterate, and why there is no executor option: an iterator or a
 * {@code Runnable} per drop is the allocation this shape exists to avoid. Compose with
 * {@link #of(PrimosStatsListener...)}.
 *
 * <p>Implementations are expected to increment counters and record timers, and to do nothing else.
 * A listener that throws is caught and logged; it never kills the node. A metrics binder must not
 * be able to stop consensus.
 *
 * <h2>It duplicates nothing {@link PrimosListener} delivers</h2>
 *
 * <p>Role changes, leader changes and peer up and down are there, and are not repeated here. A
 * binder that wants both implements both. Two ways to hear one thing is how two dashboards come to
 * disagree.
 */
public interface PrimosStatsListener {

    /**
     * A sink that does nothing, shared, so a node without a listener pays one monomorphic call.
     */
    PrimosStatsListener NONE = new PrimosStatsListener() {
    };

    /**
     * This node became leader.
     *
     * @param term          the term it won
     * @param campaignNanos how long from first wanting to lead to leading. Measured from the
     *                      <em>pre-vote</em> rather than the election, because the question an
     *                      operator is asking is how long the failover took, and a pre-vote that
     *                      had to be retried is part of that.
     */
    default void onElectionWon(long term, long campaignNanos) {
    }

    /**
     * This node stopped campaigning without winning — a lost election, or a pre-vote refused.
     *
     * <p>The twin of {@link #onElectionWon}, and the more interesting of the two when a cluster
     * will not fail over: a pre-vote refused is <em>why no election happened</em>, and nothing else
     * reports it.
     */
    default void onElectionLost(long term, long campaignNanos) {
    }

    /**
     * A leader is known again after a period with none.
     *
     * @param leaderlessNanos how long the node had no leader. Begins when the last one was lost,
     *                        so it spans the whole outage rather than one election.
     */
    default void onLeaderKnown(long term, long leaderlessNanos) {
    }

    /**
     * This node stood itself down because it could no longer reach a majority — CheckQuorum.
     *
     * <p>Worth alerting on. A leader that repeatedly relinquishes is one whose network is
     * marginal, and it is the difference between "elections happen" and "this node keeps losing
     * contact with its own cluster".
     */
    default void onLeadershipRelinquished(long term) {
    }

    /**
     * This node handed leadership to a follower as it shut down.
     *
     * <p>Against the number of times it was stopped while leading, this is how many deploys cost
     * the cluster a full election timeout with nobody in charge.
     */
    default void onHandoverSent(int toPeerId) {
    }

    /**
     * This node campaigned because a departing leader told it to.
     */
    default void onHandoverAccepted(long term) {
    }

    /**
     * A peer said it was shutting down.
     *
     * <p>Against the peer-down events: departures that were announced are orderly shutdowns, and
     * the rest were inferred from silence — a crash, a partition, or a garbage collection long
     * enough to look like one. Very different things on a dashboard.
     */
    default void onDepartureObserved(int peerId) {
    }

    /**
     * A probe round trip completed. Every sample, which is what a percentile needs.
     */
    default void onProbeRoundTrip(int peerId, long nanos) {
    }

    /**
     * A datagram was discarded, and why.
     *
     * <p>The reason is the diagnosis: a cluster silently dropping everything should be readable
     * from a counter rather than a packet capture, and {@link DropReason#BAD_TAG} against
     * {@link DropReason#WRONG_APPLICATION} separates a wrong secret from a misconfigured neighbour.
     * Called on every drop, so this is the callback most exposed to a flood.
     */
    default void onDatagramDropped(DropReason reason) {
    }

    /**
     * A peer was seen running different intervals from this node's.
     *
     * <p>Fired on the transition into disagreement, not per datagram: a peer that stays
     * misconfigured is reported once, and again only if it agrees and then diverges afresh.
     */
    default void onTimingMismatch(int peerId) {
    }

    /**
     * A peer stated intervals that could not form a valid profile, so they were ignored.
     */
    default void onUnusableNodeConfig(int peerId) {
    }

    /**
     * A witness stopped tracking a learned voter that had gone quiet. Always zero on a node with a
     * configured peer set.
     */
    default void onPeerForgotten(int peerId) {
    }

    /**
     * One sink that feeds several, since there is deliberately no list to add to.
     *
     * <p>Allocation-free per event: the array is built once, here, and walked by index.
     */
    static PrimosStatsListener of(PrimosStatsListener... listeners) {
        PrimosStatsListener[] sinks = listeners.clone();
        return new CompositeStatsListener(sinks);
    }
}
