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

import org.lolaf.primos.Role;

/**
 * What the simulation needs from a node: something it can drive, observe and kill.
 *
 * <p>Deliberately small. The harness must not know how a node works, only when it wants to run,
 * what state it is in, and how to stop it existing — otherwise the harness starts encoding the
 * algorithm's assumptions and stops being able to catch the algorithm being wrong.
 */
public interface SimNode {

    /** This node's id, unique within the simulated cluster. */
    int id();

    /** Whether this node is a witness, which must never campaign or lead. */
    boolean isWitness();

    /** Whether the node is currently up. A crashed node is skipped until it is restarted. */
    boolean isRunning();

    /**
     * When this node next wants to run, in clock nanoseconds, or {@link Long#MAX_VALUE} if it is
     * waiting only on the network. The scheduler advances time to the earliest of these and the
     * next queued datagram, so nothing is ever polled in a busy loop.
     */
    long nextDeadlineNanos();

    /** Drains whatever has arrived and fires whatever is due. */
    void tick();

    /** The role as of now. */
    Role role();

    /** The term as of now. */
    long currentTerm();

    /** Who this node voted for in {@link #currentTerm()}, or -1. */
    int votedFor();

    /**
     * Kills the node, discarding everything it had not made durable.
     *
     * <p>This is the boundary the whole crash-safety argument rests on: whatever the node did not
     * write through its store is gone. A node that acted on a term or a vote before persisting it
     * will contradict itself after {@link #restart()}, and the invariants will say so.
     */
    void crash();

    /** Brings the node back up, reading its state back from the store that outlived it. */
    void restart();
}
