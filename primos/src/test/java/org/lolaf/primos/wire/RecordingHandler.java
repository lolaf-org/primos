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

import java.util.ArrayList;
import java.util.List;

/** Records what the decoder dispatched, so tests can assert on it. */
final class RecordingHandler implements MessageHandler {

    final List<String> received = new ArrayList<>();

    @Override
    public void onRequestVote(int senderId, long term, boolean preVote) {
        received.add("requestVote(" + senderId + "," + term + ",preVote=" + preVote + ")");
    }

    @Override
    public void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote) {
        received.add("requestVoteReply(" + senderId + "," + term + ",granted=" + granted
                + ",preVote=" + preVote + ")");
    }

    @Override
    public void onAppendEntries(int senderId, long term, long leaderVersion) {
        received.add("appendEntries(" + senderId + "," + term + ",version=" + leaderVersion + ")");
    }

    @Override
    public void onAppendEntriesReply(int senderId, long term, boolean success, boolean candidate) {
        received.add("appendEntriesReply(" + senderId + "," + term + ",success=" + success
                + (candidate ? "" : ",declines") + ")");
    }

    @Override
    public void onPing(int senderId, long term, long sequence) {
        received.add("ping(" + senderId + "," + term + ",seq=" + sequence + ")");
    }

    @Override
    public void onPingReply(int senderId, long term, long sequence) {
        received.add("pingReply(" + senderId + "," + term + ",seq=" + sequence + ")");
    }

    String only() {
        if (received.size() != 1) {
            throw new AssertionError("expected exactly one message, got " + received);
        }
        return received.get(0);
    }

    @Override
    public void onTimeoutNow(int senderId, long term) {
        received.add("timeoutNow(" + senderId + "," + term + ")");
    }

    @Override
    public void onLeaving(int senderId, long term) {
        received.add("leaving(" + senderId + "," + term + ")");
    }

    @Override
    public void onNodeConfig(int senderId, long term, int heartbeatMillis, int electionMinMillis,
                             int electionMaxMillis, int probeMillis, int failureThreshold) {
        received.add("nodeConfig(" + senderId + "," + term + "," + heartbeatMillis + ","
                + electionMinMillis + "," + electionMaxMillis + "," + probeMillis + ","
                + failureThreshold + ")");
    }
}
