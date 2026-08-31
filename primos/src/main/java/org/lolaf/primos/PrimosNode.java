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

import java.util.Optional;

/**
 * One application instance's membership of a cluster: it starts, joins, and can answer
 * <em>am I the leader?</em> at any moment.
 *
 * <p>Leader election only. There is no replicated log and no state machine — your application
 * keeps its own state, and primos only tells it who is currently in charge.
 *
 * <p><strong>Using it safely.</strong> There is always a window in which this node believes it
 * leads and does not: leadership can be lost at any instant and the news travels no faster than
 * the network. That window is inherent to leader election, and it is bounded but not closed by a
 * leader standing down when it stops hearing from a majority. So {@link #role()} alone is not a
 * sufficient guard for an external side effect. Read {@link #roleAndTerm()}, pass the term to
 * whatever system the work touches, and have that system reject anything carrying a term lower
 * than the highest it has seen.
 *
 * <p>A node runs exactly one thread, and it is a daemon thread, so it never holds a JVM open.
 */
public interface PrimosNode extends AutoCloseable {

    /**
     * Starts building a node.
     */
    static PrimosNodeBuilder builder() {
        return new PrimosNodeBuilder();
    }

    /**
     * Binds the socket, reads back the persisted term and vote, and starts the node thread.
     *
     * <p>Returns once the node is running, <em>not</em> once a leader exists: the node returns as a
     * follower, and an election takes at least one election timeout. Starting an already-started
     * or stopped node throws.
     */
    void start();

    /**
     * Stops the node thread, closes the socket and flushes state, blocking until that is done.
     *
     * <p>Idempotent, because this ends up in shutdown hooks and container teardown. A stopped node
     * is terminal: build a new one rather than restarting this one.
     */
    void stop();

    /**
     * Calls {@link #stop()}, so a node can be used in try-with-resources.
     */
    @Override
    default void close() {
        stop();
    }

    /**
     * This node's current role, as a volatile read cheap enough for an application hot path.
     *
     * <p>See the class javadoc before guarding anything external on this.
     */
    Role role();

    /**
     * The role and the term it is held in, read together so the fencing token cannot be lost.
     */
    RoleAndTerm roleAndTerm();

    /**
     * Who is currently leading, if anyone is known to be. Empty during an election, which is a
     * real and common state rather than an error.
     *
     * <p>A {@link Leader} rather than a bare id, so that a follower asking where to forward work
     * is answered rather than pointed at a lookup — and so that this node holding leadership is
     * distinguishable from nobody holding it. See {@link Leader}.
     */
    Optional<Leader> leader();

    /**
     * Registers a listener. Allowed after {@link #start()}, in which case it is immediately
     * delivered a synthetic event describing the current state, so a late listener cannot miss the
     * transition it was registered for.
     */
    void addListener(PrimosListener listener);

    /**
     * An immutable snapshot of counters and per-peer observations, built when you ask for one.
     *
     * <p>Best-effort numbers: they are read from the node thread's state without co-ordinating
     * with it, so a snapshot is consistent enough for a dashboard rather than a consistent
     * instant. Never guard anything on these — that is what {@link #roleAndTerm()} is for.
     */
    PrimosStats stats();

    /**
     * Whether this node has been started and not yet stopped.
     *
     * <p>Distinguishes <em>not started yet</em> from every other state, which nothing else here
     * does: a node that has never joined reports {@link Role#FOLLOWER} exactly like a healthy one
     * that has. That matters wherever the start is deferred — a framework integration that hands
     * the application a built node and lets it choose when to join has no other way to tell a
     * deliberate wait from an application that forgot, and the second is silent.
     *
     * <p>Not a guard for anything. It is false both before {@link #start()} and after
     * {@link #stop()}, and says nothing about leadership: use {@link #roleAndTerm()} for that.
     */
    boolean isRunning();

    /**
     * The configuration this node was built with.
     */
    ClusterConfig config();
}
