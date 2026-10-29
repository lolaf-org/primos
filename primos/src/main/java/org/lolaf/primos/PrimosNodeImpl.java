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
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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

    private static final int NO_TRANSFER = Integer.MIN_VALUE;
    private static final int ANY_FOLLOWER = Integer.MIN_VALUE + 1;

    private final ClusterConfig config;
    private final Supplier<StateStore> stores;
    private final Supplier<Transport> transports;
    private final MonotonicClock clock;
    private final Random random;
    private final MessageAuthenticator authenticator;
    private final ListenerDispatcher dispatcher;
    private final long stopTimeoutNanos;

    /**
     * Where the counters go, already guarded against throwing. Held here rather than per
     * generation, so a listener registered once keeps counting across a restart.
     */
    private final PrimosStatsListener stats;
    private final Object transfer = new Object();
    private StateStore store;
    private Transport transport;
    private ElectionNode election;
    private volatile RoleAndTerm roleAndTerm;
    private volatile Leader leader;
    private volatile long leadershipConfirmedNanos = Long.MIN_VALUE;
    private volatile boolean running;
    private volatile int transferRequest = NO_TRANSFER;
    /**
     * What the application asked, applied to the election by the node thread each turn and to
     * every new election on start.
     */
    private volatile boolean candidate = true;
    private boolean transferServed;
    private boolean transferSent;
    private Thread thread;

    PrimosNodeImpl(ClusterConfig config,
                   Supplier<StateStore> stores,
                   Supplier<Transport> transports,
                   MonotonicClock clock,
                   Random random,
                   MessageAuthenticator authenticator,
                   Executor listenerExecutor,
                   long slowCallbackNanos,
                   long stopTimeoutNanos,
                   PrimosStatsListener stats) {
        this.config = config;
        this.stores = stores;
        this.transports = transports;
        this.clock = clock;
        this.random = random;
        this.authenticator = authenticator;
        this.stopTimeoutNanos = stopTimeoutNanos;
        this.stats = stats;
        this.dispatcher = new ListenerDispatcher(config.applicationName(), config.nodeId(), clock, listenerExecutor, slowCallbackNanos);
        this.roleAndTerm = new RoleAndTerm(Role.FOLLOWER, 0);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            log.info("closing a node's {} failed", resource.getClass().getSimpleName(), e);
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            // Idempotent rather than fatal, as stop() is
            return;
        }
        if (election != null && election.isFailed()) {
            throw new IllegalStateException("node " + config.nodeId() + " has failed and cannot be"
                    + " restarted: it stopped because something it depends on is broken, and"
                    + " bringing it back into the vote would not fix that. Restart the process.");
        }
        try {
            store = stores.get();
            transport = transports.get();
        } catch (RuntimeException e) {
            closeQuietly();
            throw e;
        }
        election = new ElectionNode(config, store, transport, clock, random, authenticator, this, stats);
        running = true;
        thread = new Thread(this::loop, "primos-" + config.applicationName() + "-" + config.nodeId());
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((thread, throwable) ->
                log.error("Uncaught exception occurred in node " + PrimosNodeImpl.this + ", terminal state, restart process", throwable));
        thread.start();

        log.info("cluster '{}': node {} started as {}", config.applicationName(), config.nodeId(), roleAndTerm.role());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            // Never started, or already stopped. Either way the store and the transport are this
            // node's to release: it made them, and a generation that never ran is still holding a
            // bound socket and an exclusive lock. Both closes are idempotent.
            closeQuietly();
            return;
        }
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
    public synchronized Optional<PublishedVersion> publishChange() {
        if (!running) {
            return Optional.empty();
        }
        return election.publishChange();
    }

    @Override
    public boolean transferLeadership() {
        return requestTransfer(ANY_FOLLOWER);
    }

    @Override
    public boolean transferLeadership(int targetId) {
        return requestTransfer(targetId);
    }

    /**
     * Waits for the node thread to serve the request, bounded by the stop timeout in case the
     * thread is gone. Synchronized like {@link #publishChange()}, so {@link #stop()} cannot close
     * the transport under it, and one caller at a time uses the single request slot.
     */
    private synchronized boolean requestTransfer(int target) {
        synchronized (transfer) {
            if (!running) {
                return false;
            }
            transferServed = false;
            transferRequest = target;
            transport.wakeup();
            long deadline = clock.nanoTime() + stopTimeoutNanos;
            try {
                while (!transferServed && running) {
                    long remaining = deadline - clock.nanoTime();
                    if (remaining <= 0) {
                        break;
                    }
                    TimeUnit.NANOSECONDS.timedWait(transfer, remaining);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                transferRequest = NO_TRANSFER;
            }
            return transferServed && transferSent;
        }
    }

    private void serveTransfer() {
        if (transferRequest == NO_TRANSFER) {
            return;
        }
        synchronized (transfer) {
            int target = transferRequest;
            if (target == NO_TRANSFER) {
                return;
            }
            transferRequest = NO_TRANSFER;
            transferSent = target == ANY_FOLLOWER ? election.transferLeadership() : election.transferLeadership(target);
            transferServed = true;
            transfer.notifyAll();
        }
    }

    @Override
    public void setCandidate(boolean candidate) {
        this.candidate = candidate;
        wakeUp();
    }

    private synchronized void wakeUp() {
        if (running) {
            transport.wakeup();
        }
    }

    @Override
    public boolean isCandidate() {
        return candidate && !config.isWitness();
    }

    @Override
    public OptionalLong leadershipConfirmedNanos() {
        long confirmed = leadershipConfirmedNanos;
        return running && confirmed != Long.MIN_VALUE ? OptionalLong.of(confirmed) : OptionalLong.empty();
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
        if (running) {
            // A listener registered after the node started would otherwise wait for the next
            // change to learn anything, and may have been registered for one that already happened.
            dispatcher.deliverCurrentState(listener, roleAndTerm.role(), roleAndTerm.term(), leader());
        }
    }

    @Override
    public ClusterConfig config() {
        return config;
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        roleAndTerm = new RoleAndTerm(current, term);
        dispatcher.onRoleChanged(previous, current, term);
    }

    @Override
    public void onLeaderChanged(Optional<Leader> next, long term) {
        RoleAndTerm current = roleAndTerm;
        if (current.term() != term) {
            // a follower's term moves to its leader's with no role change to carry it
            roleAndTerm = new RoleAndTerm(current.role(), term);
        }
        leader = next.orElse(null);
        dispatcher.onLeaderChanged(next, term);
    }

    @Override
    public void onLeaderVersion(Leader leader, long term, long version) {
        dispatcher.onLeaderVersion(leader, term, version);
    }

    @Override
    public void onPeerUp(Peer peer) {
        dispatcher.onPeerUp(peer);
    }

    @Override
    public void onPeerDown(Peer peer, PeerDownReason reason) {
        dispatcher.onPeerDown(peer, reason);
    }

    private void loop() {
        election.setCandidate(candidate);
        election.start();
        log.info("cluster '{}': node {} is rejoining the cluster", config.applicationName(), config.nodeId());
        while (running) {
            // No bound of our own: runOnce waits only until the next deadline, and stop() wakes
            // the socket, so there is no polling interval to tune and no latency to trade away.
            election.runOnce(Long.MAX_VALUE);
            election.setCandidate(candidate);
            serveTransfer();
            publish();
            if (election.isFailed()) {
                running = false;
            }
        }
        announceShutdown();
        publish();
    }

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

    private void standDown() {
        RoleAndTerm last = roleAndTerm;
        leader = null;
        leadershipConfirmedNanos = Long.MIN_VALUE;
        roleAndTerm = new RoleAndTerm(Role.FOLLOWER, last.term());
        if (last.role() != Role.FOLLOWER) {
            dispatcher.onRoleChanged(last.role(), Role.FOLLOWER, last.term());
        }
        dispatcher.onLeaderChanged(Optional.empty(), last.term());
    }

    private void publish() {
        RoleAndTerm published = roleAndTerm;
        if (published.role() != election.role() || published.term() != election.currentTerm()) {
            roleAndTerm = new RoleAndTerm(election.role(), election.currentTerm());
        }
        leader = election.leader();
        leadershipConfirmedNanos = election.leadershipConfirmedNanos();
    }

    @Override
    public boolean isRunning() {
        // The loop's own flag, which is already exactly "started and not stopped" and already
        // volatile because the node thread reads it. A second field would be a second thing to
        // keep in step with this one.
        return running;
    }

    private void closeQuietly() {
        closeQuietly(transport);
        closeQuietly(store);
        transport = null;
        store = null;
    }

    @Override
    public String toString() {
        return "primos-" + config.applicationName() + "-" + config.nodeId()
                + " " + roleAndTerm.role() + "/" + roleAndTerm.term();
    }
}