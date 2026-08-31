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
package org.lolaf.primos.spring;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.primos.PrimosNode;
import org.lolaf.primos.spring.PrimosProperties.NodeLifecycle;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * Starts and stops the node with the Spring context.
 *
 * <p><strong>The node is started on {@link ContextRefreshedEvent}, not from {@link #start()}, and
 * the reason is not a matter of taste.</strong> A {@code SmartLifecycle} bean that does not
 * override {@code getPhase()} sits at {@link SmartLifecycle#DEFAULT_PHASE}, which is
 * {@link Integer#MAX_VALUE} — so no phase exists that runs after an ordinary auto-starting bean,
 * and a message listener container is exactly such a bean. Refresh is therefore the only hook that
 * is genuinely after everything has auto-started, which is where the gated beans have to be
 * quiesced and where the node can first safely be told to join.
 *
 * <p>Stopping stays a {@code SmartLifecycle} concern, because that is what gives it an order: the
 * gated beans are stopped by Spring before the node is, so a departing leader stops doing the work
 * and only then hands leadership on.
 */
@Slf4j
public class PrimosNodeLifecycle implements SmartLifecycle {

    /**
     * Below {@link SmartLifecycle#DEFAULT_PHASE}, so on the way down ordinary application beans
     * stop before the node does and the handover leaves a process that has already stopped working.
     */
    public static final int PHASE = Integer.MAX_VALUE - 1000;

    private final PrimosNode node;
    private final NodeLifecycle mode;
    private final boolean gatingEnabled;
    private final LeaderLifecycleController controller;

    private volatile boolean running;

    PrimosNodeLifecycle(PrimosNode node,
                        NodeLifecycle mode,
                        boolean gatingEnabled,
                        LeaderLifecycleController controller) {
        this.node = node;
        this.mode = mode;
        this.gatingEnabled = gatingEnabled;
        this.controller = controller;
    }

    /**
     * Quiesces the gated beans, then joins the cluster if that is Spring's job.
     *
     * <p>The order is the point, and it is done by a direct call rather than by two ordered event
     * listeners so that it cannot be undone by someone reordering listeners: whatever auto-started
     * is stopped <em>before</em> anything can tell this node it is in charge. Quiescing happens in
     * both modes, because a manually started node is started later, not never.
     */
    @EventListener(ContextRefreshedEvent.class)
    void onContextRefreshed() {
        if (controller != null) {
            controller.prepare();
        }
        if (mode == NodeLifecycle.CONTEXT) {
            node.start();
            return;
        }
        log.info("cluster '{}': node {} is built but not started, because primos.lifecycle=manual."
                        + " Call PrimosNode.start() when the application is ready to lead",
                node.config().applicationName(), node.config().nodeId());
    }

    @Override
    public void start() {
        // Only marks this bean running so that Spring calls stop() on the way down. The node is
        // started from onContextRefreshed(), for the phase reason on the class.
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        // Both modes, deliberately: only the start is ever manual. This is what sends TimeoutNow to
        // a successor and Leaving to everyone, turning a deploy into about a round trip of
        // leaderlessness instead of a full election timeout — far too valuable to leave to an
        // application that has to remember. Idempotent, and a no-op on a node never started.
        node.stop();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /**
     * The one silent failure {@code manual} introduces: configured, never started, gating enabled,
     * so the gated beans wait for leadership that cannot arrive. No election, no error, and nothing
     * in any log — the application simply does nothing for ever.
     *
     * <p>Only when gating is on. Without it, a node deliberately started later is ordinary and
     * correct, and warning about it would be noise.
     */
    boolean isWaitingOnAManualStart() {
        return mode == NodeLifecycle.MANUAL && gatingEnabled && !node.isRunning();
    }

    /**
     * Runners have already executed by the time this event is published, so a node started in one
     * is started by now and this stays quiet.
     */
    @EventListener(ApplicationReadyEvent.class)
    void warnIfNeverStarted() {
        if (isWaitingOnAManualStart()) {
            log.warn("cluster '{}': node {} has not been started, and leader-gated beans are"
                            + " waiting on leadership that cannot arrive until it is. Either call"
                            + " PrimosNode.start(), or set primos.lifecycle=context to have Spring"
                            + " start it.",
                    node.config().applicationName(), node.config().nodeId());
        }
    }
}
