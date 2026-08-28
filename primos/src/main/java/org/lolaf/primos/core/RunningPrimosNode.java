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

import java.util.Objects;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.PrimosStats;
import org.lolaf.primos.Role;
import org.lolaf.primos.RoleAndTerm;
import org.lolaf.primos.store.StateStore;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
final class RunningPrimosNode implements PrimosNode, PrimosListener {

    private static final Logger log = LoggerFactory.getLogger(RunningPrimosNode.class);

    private final ClusterConfig config;
    private final StateStore store;
    private final Transport transport;
    private final ListenerDispatcher dispatcher;
    private final ElectionNode election;
    private final long stopTimeoutNanos;

    private volatile RoleAndTerm roleAndTerm = new RoleAndTerm(Role.FOLLOWER, 0);
    private volatile int leaderId = -1;
    private volatile PrimosStats stats;
    private volatile boolean running;

    private Thread thread;
    private boolean started;
    private boolean stopped;

    RunningPrimosNode(ClusterConfig config,
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
        this.election = new ElectionNode(config, store, transport, clock, random, authenticator,
                this);
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
    public OptionalInt leaderId() {
        int current = leaderId;
        return current < 0 ? OptionalInt.empty() : OptionalInt.of(current);
    }

    @Override
    public void addListener(PrimosListener listener) {
        Objects.requireNonNull(listener, "listener");
        dispatcher.add(listener);
        if (started && !stopped) {
            // A listener registered after the node started would otherwise wait for the next
            // change to learn anything, and may have been registered for one that already happened.
            dispatcher.deliverCurrentState(listener, roleAndTerm.role(), roleAndTerm.term(),
                    leaderId());
        }
    }

    @Override
    public PrimosStats stats() {
        PrimosStats current = stats;
        return current != null ? current : election.stats();
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
    public void onLeaderChanged(OptionalInt leader, long term) {
        leaderId = leader.orElse(-1);
        dispatcher.onLeaderChanged(leader, term);
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
        publish();
    }

    private void publish() {
        roleAndTerm = election.roleAndTerm();
        leaderId = election.leaderId().orElse(-1);
        stats = election.stats();
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
