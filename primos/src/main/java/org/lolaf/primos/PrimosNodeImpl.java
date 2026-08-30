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

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.core.ElectionNode;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A node with a thread: the election algorithm, run for real.
 *
 * <p>Exactly one thread, and it is a daemon, so a forgotten node never holds a JVM open. It blocks
 * in one place — the transport's poll, bounded by the next timer deadline — which is what lets the
 * whole design be single-threaded without busy-waiting.
 *
 * <p>{@link #role()} is answerable from any thread because the node publishes an immutable
 * snapshot whenever anything changes and after every turn of the loop. Application threads read a
 * volatile reference; they never touch the algorithm's state.
 */
@Slf4j
final class PrimosNodeImpl implements PrimosNode, PrimosListener {

    private final ClusterConfig config;
    private final StateStore store;
    private final Transport transport;
    private final ListenerDispatcher dispatcher;
    private final ElectionNode election;
    private final long stopTimeoutNanos;

    private volatile RoleAndTerm roleAndTerm;
    private volatile Leader leader;
    private volatile boolean running;

    private Thread thread;
    private boolean started;
    private boolean stopped;

    PrimosNodeImpl(ClusterConfig config,
                   StateStore store,
                   Transport transport,
                   MonotonicClock clock,
                   Random random,
                   MessageAuthenticator authenticator,
                   Executor listenerExecutor,
                   long slowCallbackNanos,
                   long stopTimeoutNanos) {
        this.config = config;
        this.store = store;
        this.transport = transport;
        this.stopTimeoutNanos = stopTimeoutNanos;
        this.dispatcher = new ListenerDispatcher(config.applicationName(), config.nodeId(), clock,
                listenerExecutor, slowCallbackNanos);
        this.election = new ElectionNode(config, store, transport, clock, random, authenticator, this);
        this.roleAndTerm = new RoleAndTerm(Role.FOLLOWER, 0);
    }

    @Override
    public synchronized void start() {
        if (stopped) {
            throw new IllegalStateException("node " + config.nodeId() + " has been stopped; a"
                    + " stopped node is terminal, so build a new one rather than restarting it");
        }
        if (started) {
            throw new IllegalStateException("node " + config.nodeId() + " is already started");
        }

        // Loading state on the caller's thread means a corrupt store or a locked directory is
        // reported to whoever called start(), rather than killing a thread nobody is watching.
        election.start();
        publish();
        started = true;
        running = true;

        thread = new Thread(this::loop,
                "primos-" + config.applicationName() + "-" + config.nodeId());
        thread.setDaemon(true);
        thread.start();

        log.info("cluster '{}': node {} started as {}",
                config.applicationName(), config.nodeId(), roleAndTerm.role());
    }

    @Override
    public synchronized void stop() {
        if (!started || stopped) {
            stopped = true;
            return;
        }
        stopped = true;
        running = false;
        transport.wakeup();

        try {
            thread.join(TimeUnit.NANOSECONDS.toMillis(stopTimeoutNanos));
            if (thread.isAlive()) {
                log.warn("cluster '{}': node {} did not stop within {}ms; closing its socket and"
                                + " store anyway",
                        config.applicationName(), config.nodeId(),
                        TimeUnit.NANOSECONDS.toMillis(stopTimeoutNanos));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            standDown();
            closeQuietly();
        }
        log.info("cluster '{}': node {} stopped", config.applicationName(), config.nodeId());
    }

    @Override
    public Role role() {
        return roleAndTerm.role();
    }

    @Override
    public RoleAndTerm roleAndTerm() {
        return roleAndTerm;
    }

    @Override
    public Optional<Leader> leader() {
        return Optional.ofNullable(leader);
    }

    @Override
    public void addListener(PrimosListener listener) {
        Objects.requireNonNull(listener, "listener");
        dispatcher.add(listener);
        if (started && !stopped) {
            // A listener registered after the node started would otherwise wait for the next
            // change to learn anything, and may have been registered for one that already happened.
            dispatcher.deliverCurrentState(listener, roleAndTerm.role(), roleAndTerm.term(),
                    leader());
        }
    }

    /**
     * A snapshot of the counters, built on the calling thread when it asks for one.
     *
     * <p>On demand rather than after every turn of the loop, because a snapshot is a good handful
     * of objects — a map of per-peer observations, the drop counters, two durations per peer — and
     * a leader turns its loop several times per heartbeat. Building all of that on the chance that
     * somebody might ask was most of what a node thread allocated; nobody reads stats at heartbeat
     * rate.
     *
     * <p><strong>Read without synchronisation, so the numbers are best-effort.</strong> Everything
     * here is written by the node thread, and this method deliberately does not co-ordinate with
     * it: a caller can therefore see values from slightly different instants — a role from just
     * before an election beside a term from just after — and, on a 32-bit JVM, a torn long. That
     * is a trade this method may make and nothing else in this class may: these are monitoring
     * counters, and the alternatives were allocating a snapshot nobody asked for on every turn of
     * the loop, or a handshake with the node thread costing more than the numbers are worth.
     * Nothing the algorithm depends on is read this way — the fencing token comes from
     * {@link #roleAndTerm()}, which is published as one immutable object.
     *
     * <p>What keeps this benign rather than merely cheap is that a node's peer set is fixed when it
     * is built, so the map behind the per-peer stats is never structurally modified while it is
     * read. Only the standalone witness server learns peers as it runs, and it drives its own
     * {@link ElectionNode} from its own thread.
     */
    @Override
    public PrimosStats stats() {
        return election.stats();
    }

    @Override
    public ClusterConfig config() {
        return config;
    }

    // ------------------------------------------------------------- the sink

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        roleAndTerm = new RoleAndTerm(current, term);
        dispatcher.onRoleChanged(previous, current, term);
    }

    @Override
    public void onLeaderChanged(Optional<Leader> next, long term) {
        leader = next.orElse(null);
        dispatcher.onLeaderChanged(next, term);
    }

    @Override
    public void onPeerUp(Peer peer) {
        dispatcher.onPeerUp(peer);
    }

    @Override
    public void onPeerDown(Peer peer) {
        dispatcher.onPeerDown(peer);
    }

    // ------------------------------------------------------------- the loop

    private void loop() {
        while (running) {
            // No bound of our own: runOnce waits only until the next deadline, and stop() wakes
            // the socket, so there is no polling interval to tune and no latency to trade away.
            election.runOnce(Long.MAX_VALUE);
            publish();
            if (election.isFailed()) {
                running = false;
            }
        }
        announceShutdown();
        publish();
    }

    /**
     * Hands leadership on before this thread dies, so a clean stop does not cost an election
     * timeout of leaderlessness.
     *
     * <p>Here rather than in {@link #stop()} because the transport belongs to this thread, and
     * {@code stop()} runs on the caller's. The join in {@code stop()} covers it and the socket is
     * still open, so the datagram is on its way before anything is closed.
     *
     * <p>Never allowed to throw. This runs while the node is already going away, so a failure here
     * must not become an exception on a dying thread that nobody will ever see — the cluster falls
     * back to its election timeout, which is what it would have done without any of this.
     */
    private void announceShutdown() {
        if (election.isFailed()) {
            // A failed node has no leadership to give: it stood down when it failed, and it cannot
            // vouch for a term it may not have persisted. It has already been reported down.
            return;
        }
        try {
            // Order matters. The successor should start campaigning before anything else happens,
            // and the departure notice is bookkeeping that can follow it.
            election.handOverLeadership();
            election.announceDeparture();
        } catch (RuntimeException e) {
            log.debug("cluster '{}': node {} could not announce its shutdown: {}",
                    config.applicationName(), config.nodeId(), e.toString());
        }
    }

    /**
     * Makes a stopped node stop claiming leadership.
     *
     * <p>Without this, {@link #role()} would go on returning whatever the node was doing when it
     * was stopped, and an application that checked it after shutdown would believe it was still in
     * charge — the same lie as a partitioned leader that never stands down, arrived at by being
     * switched off. A stopped node reports {@code FOLLOWER}: it leads nothing, and being terminal,
     * it never will again.
     */
    private void standDown() {
        RoleAndTerm last = roleAndTerm;
        leader = null;
        roleAndTerm = new RoleAndTerm(Role.FOLLOWER, last.term());
        if (last.role() != Role.FOLLOWER) {
            dispatcher.onRoleChanged(last.role(), Role.FOLLOWER, last.term());
        }
        dispatcher.onLeaderChanged(Optional.empty(), last.term());
    }

    /**
     * Republishes what an application thread may read, after every turn of the loop.
     *
     * <p>Every turn, so it has to be cheap: a turn that changed nothing allocates nothing. The
     * role and term are only re-boxed into a {@link RoleAndTerm} when one of them has actually
     * moved, and stats are not published at all — {@link #stats()} builds those when asked.
     */
    private void publish() {
        RoleAndTerm published = roleAndTerm;
        Role role = election.role();
        long term = election.currentTerm();
        if (published.role() != role || published.term() != term) {
            roleAndTerm = new RoleAndTerm(role, term);
        }
        leader = election.leaderOrNull();
    }

    private void closeQuietly() {
        try {
            transport.close();
        } catch (RuntimeException e) {
            log.debug("closing the transport failed", e);
        }
        try {
            store.close();
        } catch (RuntimeException e) {
            log.debug("closing the store failed", e);
        }
    }

    @Override
    public String toString() {
        return "primos-" + config.applicationName() + "-" + config.nodeId()
                + " " + roleAndTerm.role() + "/" + roleAndTerm.term();
    }
}
