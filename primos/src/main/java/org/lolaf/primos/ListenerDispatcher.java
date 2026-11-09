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
import org.lolaf.primos.time.MonotonicClock;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Delivers events to the application's listeners through one executor, which is what keeps them in order and off the
 * node thread. A watchdog warns about a callback that runs long, naming the listener, and an exception from one is
 * logged and swallowed, so it can neither kill the node nor stop the other listeners hearing about the event.
 */
@Slf4j
final class ListenerDispatcher implements PrimosListener {

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
    void deliverCurrentState(PrimosListener listener, Role role, long term,
                             Optional<Leader> leader) {
        deliverTo(listener, "onRoleChanged", () -> listener.onRoleChanged(role, role, term));
        deliverTo(listener, "onLeaderChanged", () -> listener.onLeaderChanged(leader, term));
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        each("onRoleChanged", listener -> listener.onRoleChanged(previous, current, term));
    }

    @Override
    public void onLeaderChanged(Optional<Leader> leader, long term) {
        each("onLeaderChanged", listener -> listener.onLeaderChanged(leader, term));
    }

    @Override
    public void onLeaderVersion(Leader leader, long term, long version) {
        each("onLeaderVersion", listener -> listener.onLeaderVersion(leader, term, version));
    }

    @Override
    public void onPeerUp(Peer peer) {
        each("onPeerUp", listener -> listener.onPeerUp(peer));
    }

    @Override
    public void onPeerDown(Peer peer, PeerDownReason reason) {
        each("onPeerDown", listener -> listener.onPeerDown(peer, reason));
    }

    private void each(String event, Consumer<PrimosListener> delivery) {
        for (PrimosListener listener : listeners) {
            deliverTo(listener, event, () -> delivery.accept(listener));
        }
    }

    private void deliverTo(PrimosListener listener, String event, Runnable delivery) {
        try {
            executor.execute(() -> invoke(listener, event, delivery));
        } catch (RuntimeException rejected) {
            log.warn("cluster '{}': node {} listener {} missed {}: its executor refused the task: {}",
                    cluster, nodeId, listener.getClass().getName(), event, rejected.toString());
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
            log.warn("cluster '{}': node {} listener {} took {}ms in {}. Callbacks run one at a time, so a"
                            + " slow one delays every event behind it. Hand the work to your own executor.",
                    cluster, nodeId, listener.getClass().getName(),
                    TimeUnit.NANOSECONDS.toMillis(tookNanos), event);
        }
    }
}
