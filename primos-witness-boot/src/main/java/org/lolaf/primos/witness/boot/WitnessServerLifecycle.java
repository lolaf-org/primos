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
package org.lolaf.primos.witness.boot;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;

/**
 * Runs the witness on a thread of its own, and stops it when Spring shuts the context down.
 *
 * <p>{@link WitnessServer#run()} blocks its caller — it is a select loop — so something has to own
 * a thread for it. This is the whole of what this class does; it is deliberately the same two calls
 * the tarball's main makes, driven by a different lifecycle. <strong>No decision about serving is
 * taken here</strong>, because a second front door that grows logic is a second server.
 *
 * <p><strong>Stop asks and then waits, rather than closing anything itself.</strong> Closing a
 * state store from the shutdown thread while the server thread is mid-write is exactly the
 * corruption the store exists to prevent, and without the join the context would carry on
 * destroying beans — the server's own {@code close()} among them — under a loop still running.
 * That close is what sends each served cluster its {@code Leaving} datagram, which is worth the
 * wait: a cluster whose witness has silently gone is back to the fault tolerance it has without
 * one, and the sooner it knows the sooner somebody looks.
 *
 * <p>The wait is bounded by {@code spring.lifecycle.timeout-per-shutdown-phase}, the same budget
 * Spring gives this phase, so the two cannot disagree. A container's stop grace period must exceed
 * it — see the image's documentation, where the default 10s on both sides means a clean stop can
 * be killed at the instant it finishes.
 */
@Slf4j
public class WitnessServerLifecycle implements SmartLifecycle {

    private final WitnessServer server;
    private final Duration stopTimeout;

    /**
     * The loop's thread, or null before the first start. Volatile because {@link #isRunning()} is
     * answered from whichever thread asks — an actuator endpoint, most often.
     */
    private volatile Thread thread;

    public WitnessServerLifecycle(WitnessServer server, Duration stopTimeout) {
        this.server = server;
        this.stopTimeout = stopTimeout;
    }

    @Override
    public void start() {
        if (isRunning()) {
            return;
        }
        Thread loop = new Thread(this::serve, "primos-witness");
        loop.setUncaughtExceptionHandler((thread, throwable) ->
                log.error("Uncaught exception occurred in thread {}", thread, throwable));
        thread = loop;
        loop.start();
    }

    @Override
    public void stop() {
        Thread loop = thread;
        if (loop == null) {
            return;
        }
        server.stop();
        try {
            loop.join(stopTimeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (loop.isAlive()) {
            log.warn("the witness loop did not finish within {}; the context will carry on"
                            + " shutting down and the departure datagrams may not have been sent",
                    stopTimeout);
        }
        thread = null;
    }

    /**
     * Whether the loop is actually running, rather than whether it was asked to.
     *
     * <p>The distinction is the point: if the thread dies, this must stop saying yes. The library
     * holds itself to the same rule for a node whose thread has gone — a component that reports
     * health from an intention rather than from a fact is how a dead server keeps its place in a
     * rotation.
     */
    @Override
    public boolean isRunning() {
        Thread loop = thread;
        return loop != null && loop.isAlive();
    }

    /**
     * Runs the loop, and makes sure a thread that dies says so.
     *
     * <p>{@link WitnessServer} quarantines an application whose node fails and keeps serving the
     * rest, so anything reaching here is the loop itself failing — every served cluster at once.
     * That must be loud, and it must leave {@link #isRunning()} answering no.
     */
    private void serve() {
        try {
            server.run();
        } catch (RuntimeException | Error e) {
            log.error("the witness loop has stopped; this witness is serving nobody until it is"
                    + " restarted, and every cluster relying on it is back to the fault tolerance"
                    + " it has without one", e);
            throw e;
        }
    }
}
