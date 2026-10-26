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
package org.lolaf.primos.wire;

/**
 * Receives decoded messages.
 *
 * <p>Fields are handed over as primitives rather than as a message object, so the receive path
 * allocates nothing without needing any machinery to arrange it.
 *
 * <p>Every method gets the sender and the term, because the rule that a message bearing a higher
 * term means "adopt it and step down" applies to all six alike.
 */
public interface MessageHandler {

    /**
     * A vote request. When {@code preVote} is set the caller is asking whether a vote
     * <em>would</em> be granted, and answering must not touch {@code currentTerm} or
     * {@code votedFor} — so it costs no fsync, and a partitioned node cannot inflate its term.
     */
    void onRequestVote(int senderId, long term, boolean preVote);

    /**
     * A vote reply. Count it only while still a candidate in this exact term, and dedupe by sender.
     */
    void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote);

    /**
     * A heartbeat from a leader, with the version it last published in this term.
     */
    void onAppendEntries(int senderId, long term, long leaderVersion);

    /**
     * A heartbeat acknowledgement. Its arrival is also what feeds CheckQuorum.
     *
     * @param candidate whether the sender stands for election
     */
    void onAppendEntriesReply(int senderId, long term, boolean success, boolean candidate);

    /**
     * A reachability probe, to be answered with the same sequence number.
     */
    void onPing(int senderId, long term, long sequence);

    /**
     * A probe reply. Ignore it if the sequence is older than the failure threshold allows.
     */
    void onPingReply(int senderId, long term, long sequence);

    /**
     * A leader that is shutting down cleanly, telling this node to campaign immediately.
     *
     * <p>Raft thesis §3.10. The alternative is silence: a leader that simply stops leaves the
     * cluster without one until somebody's election timeout expires, and this turns that wait into
     * about one round trip.
     */
    void onTimeoutNow(int senderId, long term);

    /**
     * A peer announcing that it is shutting down.
     *
     * <p>A reachability hint and nothing more. Silence is the only other way to learn this, and
     * silence has to be given a long benefit of the doubt on the JVM, because a stop-the-world
     * pause looks exactly like a dead node.
     */
    void onLeaving(int senderId, long term);

    /**
     * A peer stating its own configuration: the intervals it is running.
     *
     * <p>Every value is milliseconds. <strong>Between nodes: report, never act.</strong> The timings of a cluster
     * must match for failover to be prompt — a node with a longer {@code electionTimeoutMin} is
     * proportionally more reluctant to grant the pre-vote a failover waits on — but nothing about a
     * mismatch is unsafe, and a node that changed its behaviour on seeing one would turn a rolling
     * configuration change into a partition.
     */
    void onNodeConfig(int senderId, long term, int heartbeatMillis, int electionMinMillis,
                      int electionMaxMillis, int probeMillis, int failureThreshold);
}
