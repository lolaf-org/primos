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

import org.lolaf.primos.time.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Delivers events to the application's listeners.
 *
 * <p>It implements {@link PrimosListener} itself, so it plugs into the node as the single sink the
 * algorithm was written against and fans out from there.
 *
 * <p><strong>Callbacks run on the node thread by default</strong>, which is the price of a node
 * that owns exactly one thread. A listener that blocks therefore stalls consensus, misses
 * heartbeats and can trigger a spurious election, so three things guard against that being
 * mysterious when it happens:
 *
 * <ul>
 *   <li>a watchdog times every callback and warns when one runs long — the failure becomes a log
 *       line naming the listener, instead of unexplained election flapping;</li>
 *   <li>an {@link Executor} can be supplied, moving dispatch off the node thread entirely without
 *       primos creating a thread of its own;</li>
 *   <li>an exception from one listener is logged and swallowed, so it can neither kill the node
 *       nor stop the other listeners hearing about the event.</li>
 * </ul>
 */
final class ListenerDispatcher implements PrimosListener {

    private static final Logger log = LoggerFactory.getLogger(ListenerDispatcher.class);

    private final List<PrimosListener> listeners = new CopyOnWriteArrayList<>();
    private final MonotonicClock clock;
    private final Executor executor;
    private final long slowCallbackNanos;
    private final String cluster;
    private final int nodeId;

    ListenerDispatcher(String cluster, int nodeId, MonotonicClock clock, Executor executor,
                       long slowCallbackNanos) {
        this.cluster = cluster;
        this.nodeId = nodeId;
        this.clock = clock;
        this.executor = executor;
        this.slowCallbackNanos = slowCallbackNanos;
    }

    void add(PrimosListener listener) {
        listeners.add(listener);
    }

    void remove(PrimosListener listener) {
        listeners.remove(listener);
    }

    boolean isEmpty() {
        return listeners.isEmpty();
    }

    /**
     * Tells one listener what it missed.
     *
     * <p>A listener registered after the node started would otherwise wait for the next change to
     * learn anything, and the transition it was registered for may already have happened.
     */
    void deliverCurrentState(PrimosListener listener, Role role, long term, OptionalInt leader) {
        deliverTo(listener, "onRoleChanged", () -> listener.onRoleChanged(role, role, term));
        deliverTo(listener, "onLeaderChanged", () -> listener.onLeaderChanged(leader, term));
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        each("onRoleChanged", listener -> listener.onRoleChanged(previous, current, term));
    }

    @Override
    public void onLeaderChanged(OptionalInt leaderId, long term) {
        each("onLeaderChanged", listener -> listener.onLeaderChanged(leaderId, term));
    }

    @Override
    public void onPeerUp(Peer peer) {
        each("onPeerUp", listener -> listener.onPeerUp(peer));
    }

    @Override
    public void onPeerDown(Peer peer) {
        each("onPeerDown", listener -> listener.onPeerDown(peer));
    }

    private void each(String event, java.util.function.Consumer<PrimosListener> delivery) {
        for (PrimosListener listener : listeners) {
            deliverTo(listener, event, () -> delivery.accept(listener));
        }
    }

    private void deliverTo(PrimosListener listener, String event, Runnable delivery) {
        if (executor != null) {
            executor.execute(() -> invoke(listener, event, delivery));
        } else {
            invoke(listener, event, delivery);
        }
    }

    private void invoke(PrimosListener listener, String event, Runnable delivery) {
        long startedAt = clock.nanoTime();
        try {
            delivery.run();
        } catch (Throwable failure) {
            log.error("cluster '{}': node {} listener {} threw from {}; the event is delivered to"
                            + " the other listeners regardless",
                    cluster, nodeId, listener.getClass().getName(), event, failure);
            return;
        }
        long tookNanos = clock.nanoTime() - startedAt;
        if (tookNanos > slowCallbackNanos) {
            log.warn("cluster '{}': node {} listener {} took {}ms in {}. Callbacks run on the node"
                            + " thread, so a slow one stalls consensus and can cause a spurious"
                            + " election. Hand the work to your own executor, or give the node an"
                            + " Executor for listener dispatch.",
                    cluster, nodeId, listener.getClass().getName(),
                    TimeUnit.NANOSECONDS.toMillis(tookNanos), event);
        }
    }
}
