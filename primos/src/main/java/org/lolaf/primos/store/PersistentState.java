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
package org.lolaf.primos.store;

import org.lolaf.primos.Peer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Everything primos has to remember across a restart: the current term, and who this node voted
 * for in it.
 *
 * <p>Two fields, and they are the whole safety argument. A node that forgets its vote can grant a
 * second one in the same term, and two nodes then win the same election. That is why they are
 * always written together and never separately: a crash between "wrote the term" and "wrote the
 * vote" leaves a node that has forgotten who it voted for in the term it is now in, which is the
 * same bug reached by a side door.
 */
@Getter
@EqualsAndHashCode
@ToString
public final class PersistentState {

    /** {@link #votedFor()} when this node has not voted in {@link #currentTerm()}. */
    public static final int NO_VOTE = -1;

    private static final PersistentState FRESH = new PersistentState(0L, NO_VOTE);

    private final long currentTerm;
    private final int votedFor;

    public PersistentState(long currentTerm, int votedFor) {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must not be negative, but was " + currentTerm);
        }
        if (votedFor != NO_VOTE) {
            Peer.requireValidId(votedFor, "votedFor");
        }
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    /**
     * A node that has never run: term zero, no vote. This is what a missing state file means, and
     * it is the one case where absent state is correct rather than alarming.
     */
    public static PersistentState fresh() {
        return FRESH;
    }

    /** Whether a vote has been cast in {@link #currentTerm()}. */
    public boolean hasVoted() {
        return votedFor != NO_VOTE;
    }
}
