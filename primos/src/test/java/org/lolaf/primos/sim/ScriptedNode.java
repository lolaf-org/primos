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
package org.lolaf.primos.sim;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;

import org.lolaf.primos.Role;
import org.lolaf.primos.store.PersistentState;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.Transport;

/**
 * A stand-in node, so the harness can be tested before there is an algorithm to run in it.
 *
 * <p>It exists mainly to be made to misbehave. An invariant that has never fired is not a check,
 * it is a comment, so every safety property is proved to catch a node that deliberately breaks it.
 *
 * <p>The safe and unsafe mutators are the crash-safety boundary made explicit: {@code voteFor}
 * persists before changing memory, {@code voteForWithoutPersisting} does not, and the difference
 * only becomes visible after a crash.
 */
final class ScriptedNode implements SimNode {

    private final int id;
    private final boolean witness;
    private final VirtualClock clock;
    private final StateStore store;
    private final long periodNanos;
    private final Consumer<ScriptedNode> onTick;
    private final Transport transport;
    private final int[] peers;

    private Role role = Role.FOLLOWER;
    private long term;
    private int votedFor = PersistentState.NO_VOTE;
    private boolean running = true;
    private long nextDeadline;
    private int received;

    ScriptedNode(int id, boolean witness, VirtualClock clock, StateStore store,
                 Duration period, Consumer<ScriptedNode> onTick,
                 Transport transport, int... peers) {
        this.id = id;
        this.witness = witness;
        this.clock = clock;
        this.store = store;
        this.periodNanos = period.toNanos();
        this.onTick = onTick;
        this.transport = transport;
        this.peers = peers;
        this.nextDeadline = periodNanos;
        reload();
    }

    static ScriptedNode idle(int id, VirtualClock clock, StateStore store) {
        return new ScriptedNode(id, false, clock, store, Duration.ofMillis(10), node -> {
        }, null);
    }

    static ScriptedNode idleWitness(int id, VirtualClock clock, StateStore store) {
        return new ScriptedNode(id, true, clock, store, Duration.ofMillis(10), node -> {
        }, null);
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public boolean isWitness() {
        return witness;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public long nextDeadlineNanos() {
        return nextDeadline;
    }

    @Override
    public void tick() {
        if (transport != null) {
            transport.poll(0, datagram -> received++);
        }
        while (clock.nanoTime() >= nextDeadline) {
            nextDeadline += periodNanos;
            onTick.accept(this);
            if (transport != null) {
                for (int peer : peers) {
                    transport.send(peer, ByteBuffer.wrap(("n" + id + "@" + term).getBytes()));
                }
            }
        }
    }

    @Override
    public Role role() {
        return role;
    }

    @Override
    public long currentTerm() {
        return term;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    @Override
    public void crash() {
        running = false;
        role = Role.FOLLOWER;
        term = -1;
        votedFor = PersistentState.NO_VOTE;
    }

    @Override
    public void restart() {
        reload();
        role = Role.FOLLOWER;
        running = true;
        nextDeadline = clock.nanoTime() + periodNanos;
    }

    /** Advances the term, persisting before acting on it. */
    void bumpTerm() {
        term++;
        votedFor = PersistentState.NO_VOTE;
        store.save(term, votedFor);
    }

    /** Advances the term in memory only, which is the bug a crash exposes. */
    void bumpTermWithoutPersisting() {
        term++;
        votedFor = PersistentState.NO_VOTE;
    }

    /** Grants a vote, persisting before acting on it. */
    void voteFor(int candidate) {
        votedFor = candidate;
        store.save(term, votedFor);
    }

    /** Grants a vote in memory only, so a crash lets the same term be voted in twice. */
    void voteForWithoutPersisting(int candidate) {
        votedFor = candidate;
    }

    void becomeLeader() {
        role = Role.LEADER;
    }

    void becomeCandidate() {
        role = Role.CANDIDATE;
    }

    void becomeFollower() {
        role = Role.FOLLOWER;
    }

    /** Sets the term directly, for tests that need a specific shape rather than a history. */
    void forceTerm(long newTerm) {
        term = newTerm;
    }

    int datagramsReceived() {
        return received;
    }

    private void reload() {
        PersistentState state = store.load();
        term = state.currentTerm();
        votedFor = state.votedFor();
    }

    @Override
    public String toString() {
        return "n" + id + (witness ? "(witness)" : "") + " " + role + "/" + term;
    }
}
