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
package org.lolaf.primos.core;

import java.util.ArrayList;
import java.util.List;

import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.lolaf.primos.wire.MessageDecoder;
import org.lolaf.primos.wire.MessageHandler;

/** Reads what a node actually put on the wire, so a test can assert on a reply rather than infer it. */
final class CapturedReply implements MessageHandler {

    private final MessageDecoder decoder;
    private final List<String> messages = new ArrayList<>();

    private Boolean voteGranted;
    private Boolean wasPreVote;
    private long replyTerm = -1;

    CapturedReply(String applicationName) {
        this.decoder = new MessageDecoder(applicationName, MessageAuthenticator.disabled(),
                id -> true, new org.lolaf.primos.RecordingStats());
    }

    /** Drains a peer's mailbox, remembering the last vote reply seen. */
    CapturedReply drain(Transport peer) {
        peer.poll(0, datagram -> decoder.decode(datagram, this));
        return this;
    }

    boolean sawVoteReply() {
        return voteGranted != null;
    }

    boolean granted() {
        if (voteGranted == null) {
            throw new AssertionError("no vote reply was sent; saw " + messages);
        }
        return voteGranted;
    }

    boolean preVote() {
        return Boolean.TRUE.equals(wasPreVote);
    }

    long replyTerm() {
        return replyTerm;
    }

    List<String> messages() {
        return messages;
    }

    @Override
    public void onRequestVote(int senderId, long term, boolean preVote) {
        messages.add("requestVote/" + term + (preVote ? "/pre" : ""));
    }

    @Override
    public void onRequestVoteReply(int senderId, long term, boolean granted, boolean preVote) {
        messages.add("requestVoteReply/" + term + "/" + granted + (preVote ? "/pre" : ""));
        voteGranted = granted;
        wasPreVote = preVote;
        replyTerm = term;
    }

    @Override
    public void onAppendEntries(int senderId, long term, long leaderVersion) {
        messages.add("appendEntries/" + term);
    }

    @Override
    public void onAppendEntriesReply(int senderId, long term, boolean success, boolean candidate) {
        messages.add("appendEntriesReply/" + term + "/" + success);
    }

    @Override
    public void onPing(int senderId, long term, long sequence) {
        messages.add("ping/" + sequence);
    }

    @Override
    public void onPingReply(int senderId, long term, long sequence) {
        messages.add("pingReply/" + sequence);
    }

    @Override
    public void onTimeoutNow(int senderId, long term) {
        messages.add("timeoutNow(" + senderId + "," + term + ")");
    }

    @Override
    public void onLeaving(int senderId, long term) {
        messages.add("leaving(" + senderId + "," + term + ")");
    }

    @Override
    public void onNodeConfig(int senderId, long term, int heartbeatMillis, int electionMinMillis,
                             int electionMaxMillis, int probeMillis, int failureThreshold) {
        messages.add("nodeConfig(" + senderId + "," + term + "," + heartbeatMillis + ","
                + electionMinMillis + "," + electionMaxMillis + "," + probeMillis + ","
                + failureThreshold + ")");
    }
}
