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
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.Role;
import org.lolaf.primos.spring.PrimosProperties.LeaderLifecycle;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.Lifecycle;
import org.springframework.context.LifecycleProcessor;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Starts beans as this node becomes leader and stops them as it loses leadership.
 *
 * <p><strong>It makes leadership convenient, not safe.</strong> Between losing leadership and the
 * stop completing, in-flight work is still running, and a {@link Lifecycle} stop cannot be fenced.
 * The rule is unchanged and has to be repeated precisely because this class is the thing most
 * likely to be mistaken for a solution to it: carry the term into whatever external system the work
 * touches, and have that system reject anything below the highest term it has seen.
 *
 * <p>Four things it has to get right, three of which fail silently.
 *
 * <p><strong>Gated beans must not be running when the context finishes refreshing.</strong> A
 * {@link SmartLifecycle} bean that auto-starts is already running before the first election has
 * concluded, so a follower would be consuming from its queues within a second of boot with nothing
 * anywhere to say so. This class runs at a phase just below {@link PrimosNodeLifecycle}, and its
 * own {@code start()} stops whatever auto-started — so the beans are quiesced before the node can
 * possibly be told it leads, in both node lifecycle modes.
 *
 * <p><strong>Transitions never run on the node thread.</strong> Stopping a message listener
 * container drains it and can take tens of seconds; on the node thread that stalls consensus,
 * misses heartbeats and triggers a spurious election — which loses leadership, which triggers
 * another stop. A single-threaded executor is also what stops a start and a stop from interleaving.
 *
 * <p><strong>Stale transitions are dropped by term.</strong> Leadership can be lost and regained
 * faster than a stop completes, and a stop from term 5 landing after the start for term 6 would
 * leave a leader with everything switched off — silent, indefinite, and the worst failure this
 * class can have.
 *
 * <p><strong>It stops beans, never the context.</strong> Even under {@code scope=context}: calling
 * {@code ApplicationContext.stop()} from inside the lifecycle that is currently starting would be
 * re-entrant, and stopping the same set of beans directly has the same observable effect without
 * that hazard. The scope selects which beans are gated and nothing else.
 */
@Slf4j
public class LeaderLifecycleController implements PrimosListener, SmartLifecycle {

    /**
     * Just below {@link PrimosNodeLifecycle#PHASE}, so gated beans are quiesced before the node
     * starts and this outlives the node on the way down.
     */
    public static final int PHASE = PrimosNodeLifecycle.PHASE - 1000;

    private final ListableBeanFactory beans;
    private final LeaderLifecycle config;
    private final ExecutorService transitions;

    private volatile boolean running;

    /**
     * Confined to the transition thread — every one of them, with no startup exception. That is
     * what {@link #prepare()} blocking on the executor buys, and it is the whole reason it does.
     */
    private List<GatedBean> gated = List.of();
    private long appliedTerm = Long.MIN_VALUE;
    private boolean beansRunning;

    LeaderLifecycleController(ListableBeanFactory beans, LeaderLifecycle config) {
        this.beans = beans;
        this.config = config;
        this.transitions = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "primos-leader-lifecycle");
            t.setUncaughtExceptionHandler((thread, throwable) ->
                    log.error("Uncaught exception occurred in thread {}", thread, throwable));
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Beans that must never be gated, however wide the scope.
     *
     * <p>Two of them are primos' own: gating the node on leadership would stop the very thing that
     * reports it. The third is the one that had to be found rather than predicted — Spring's
     * {@link LifecycleProcessor} is itself a {@link Lifecycle} bean, so a sweep of every
     * {@code Lifecycle} in the context picks it up, and stopping it stops <em>every</em> lifecycle
     * bean there is. That is {@code context.stop()} by another name, re-entrant, and it takes the
     * node down with it while the context is still refreshing.
     */
    private static boolean isInfrastructure(Lifecycle lifecycle) {
        return lifecycle instanceof LifecycleProcessor
                || lifecycle instanceof PrimosNodeLifecycle
                || lifecycle instanceof LeaderLifecycleController;
    }

    private static String names(List<GatedBean> gated) {
        List<String> names = new ArrayList<>();
        gated.forEach(bean -> names.add(bean.name()));
        return String.join(", ", names);
    }

    /**
     * Resolves the gated beans and stops whatever auto-started, before anything can tell this node
     * it leads.
     *
     * <p>Called by {@link PrimosNodeLifecycle} from {@link
     * org.springframework.context.event.ContextRefreshedEvent} rather than from {@link #start()},
     * and the reason is on that class: a {@code SmartLifecycle} bean that does not set a phase sits
     * at {@link Integer#MAX_VALUE}, so no phase can be ordered after one. Refresh is the only hook
     * that is genuinely after every auto-start.
     *
     * <p><strong>The work happens on the transition thread and this blocks until it is done</strong>,
     * rather than running on the caller's thread. Without that, {@code gated}, {@code appliedTerm}
     * and {@code beansRunning} would be written here and read there with no happens-before between
     * them in every case. Under {@code primos.lifecycle=context} there is a chain — refresh starts
     * the node thread, which hands to the listener executor, which hands here — but under
     * {@code manual} the application may call {@code start()} from any thread it likes, and then
     * nothing orders these writes against those reads. Blocking costs nothing: this runs once, and
     * the work is a bean lookup and the {@code stop()} calls that would have run on this thread
     * anyway.
     */
    void prepare() {
        try {
            transitions.submit(this::doPrepare).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while preparing the leader-gated beans", e);
        } catch (ExecutionException e) {
            // A misconfigured bean name is the realistic cause, and it should fail the context
            // wearing its own message rather than one wrapped in a threading detail.
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("could not prepare the leader-gated beans", e.getCause());
        }
    }

    private void doPrepare() {
        gated = resolveGatedBeans();
        if (gated.isEmpty()) {
            log.warn("primos.leader-lifecycle.enabled is true but no beans are gated on leadership."
                    + " Mark a Lifecycle bean @LeaderOnly, or name beans in"
                    + " primos.leader-lifecycle.beans.");
        } else {
            log.info("leadership gates {} bean(s): {}", gated.size(), names(gated));
        }
        if (config.scope() == LeaderLifecycle.Scope.CONTEXT) {
            log.warn("primos.leader-lifecycle.scope=context gates every Lifecycle bean in this"
                    + " context. Losing leadership will stop the web server and the actuator with"
                    + " everything else, so this application will stop answering health checks at"
                    + " exactly the moment somebody wants to read one.");
        }
        quiesce();
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        transitions.shutdown();
        try {
            // Long enough for a transition already in flight to finish, so the beans are stopped
            // before Spring destroys them rather than during.
            if (!transitions.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("a leadership transition was still running after 10s; shutting down anyway");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        submit(current == Role.LEADER, term);
    }

    private void submit(boolean shouldRun, long term) {
        try {
            transitions.execute(() -> apply(shouldRun, term));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // The context is shutting down and the executor is already closed. The beans are about
            // to be stopped by Spring regardless, so this is not worth more than a line.
            log.debug("dropping a leadership transition for term {}: {}", term, e.toString());
        }
    }

    private void apply(boolean shouldRun, long term) {
        if (term < appliedTerm) {
            log.debug("ignoring a transition from term {}, already at term {}", term, appliedTerm);
            return;
        }
        appliedTerm = term;
        if (shouldRun == beansRunning) {
            return;
        }
        beansRunning = shouldRun;
        if (shouldRun) {
            log.info("this node leads in term {}; starting {} gated bean(s)", term, gated.size());
            gated.forEach(GatedBean::start);
        } else {
            log.info("this node does not lead in term {}; stopping {} gated bean(s)",
                    term, gated.size());
            // Reverse order, so a bean that was started after another is stopped before it.
            for (int i = gated.size() - 1; i >= 0; i--) {
                gated.get(i).stop();
            }
        }
    }

    private void quiesce() {
        beansRunning = false;
        for (int i = gated.size() - 1; i >= 0; i--) {
            GatedBean bean = gated.get(i);
            if (bean.lifecycle().isRunning()) {
                log.info("stopping '{}' until this node is elected: a gated bean that auto-starts"
                        + " is already running before the first election has concluded", bean.name());
                bean.stop();
            }
        }
    }

    private List<GatedBean> resolveGatedBeans() {
        Map<String, Lifecycle> candidates = new LinkedHashMap<>();
        if (config.scope() == LeaderLifecycle.Scope.CONTEXT) {
            candidates.putAll(beans.getBeansOfType(Lifecycle.class, false, false));
        } else if (!config.beans().isEmpty()) {
            for (String name : config.beans()) {
                candidates.put(name, beans.getBean(name, Lifecycle.class));
            }
        } else {
            beans.getBeansWithAnnotation(LeaderOnly.class).forEach((name, bean) -> {
                if (bean instanceof Lifecycle lifecycle) {
                    candidates.put(name, lifecycle);
                } else {
                    log.warn("'{}' is annotated @LeaderOnly but is not a Lifecycle bean, so there"
                            + " is nothing to start or stop. Implement Lifecycle or"
                            + " SmartLifecycle.", name);
                }
            });
        }
        List<GatedBean> resolved = new ArrayList<>();
        candidates.forEach((name, lifecycle) -> {
            if (!isInfrastructure(lifecycle)) {
                resolved.add(new GatedBean(name, lifecycle));
            }
        });
        return List.copyOf(resolved);
    }

    /**
     * One gated bean, kept with its name so every log line can say which bean it means.
     */
    private record GatedBean(String name, Lifecycle lifecycle) {

        void start() {
            try {
                lifecycle.start();
            } catch (RuntimeException e) {
                // One bean failing must not abandon the rest, and must never reach the node.
                log.error("starting gated bean '{}' failed", name, e);
            }
        }

        void stop() {
            try {
                lifecycle.stop();
            } catch (RuntimeException e) {
                log.error("stopping gated bean '{}' failed", name, e);
            }
        }
    }
}
