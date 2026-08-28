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

    /** A vote reply. Count it only while still a candidate in this exact term, and dedupe by sender. */
    void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote);

    /** A heartbeat from a leader. */
    void onAppendEntries(int senderId, long term);

    /** A heartbeat acknowledgement. Its arrival is also what feeds CheckQuorum. */
    void onAppendEntriesReply(int senderId, long term, boolean success);

    /** A reachability probe, to be answered with the same sequence number. */
    void onPing(int senderId, long term, long sequence);

    /** A probe reply. Ignore it if the sequence is older than the failure threshold allows. */
    void onPingReply(int senderId, long term, long sequence);
}
