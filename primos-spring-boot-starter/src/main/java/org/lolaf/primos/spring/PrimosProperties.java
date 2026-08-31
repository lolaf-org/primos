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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Everything under {@code primos.*}.
 *
 * <p>A record with constructor binding rather than a bean with setters, for a reason worth knowing
 * before someone "fixes" it: this project configures Lombok with fluent accessors, so a generated
 * getter is {@code applicationName()} and Spring's JavaBean binder would not find it. Constructor
 * binding sidesteps the collision entirely and gives immutable configuration into the bargain.
 *
 * @param applicationName which cluster this is. Nothing is auto-configured until it is set, which
 *                        is what lets the module sit on a classpath doing nothing.
 * @param nodeId          this node's id, unique within the application
 * @param stateDirectory  where the term and vote are kept. Required, and deliberately without a
 *                        default: a default under a temp directory is silently unsafe after a
 *                        reboot, which is worse than the friction of naming the path.
 * @param bindAddress     the local address to bind, or null for the wildcard
 * @param bindPort        the local port. Every node must be reachable on the port its peers name.
 * @param peers           the other application instances, as static configuration
 * @param witness         the optional witness, at most one
 * @param secret          the shared cluster secret. Strongly recommended: without one a forged
 *                        reply is a route to a leader no majority elected.
 * @param macAlgorithm    which JCE {@code Mac} signs datagrams; defaults to HMAC-SHA256. Local
 *                        policy every member must share.
 * @param timings         a named profile, or the intervals spelled out — see {@link TimingsProperties}
 * @param voteLogging     whether who voted for whom is logged at INFO rather than DEBUG. On by
 *                        default, and silent while a leader is healthy, because a cluster that is
 *                        not electing says nothing at all.
 * @param lifecycle       when the node itself starts — see {@link NodeLifecycle}
 * @param leaderLifecycle whether leadership starts and stops beans, and which ones
 */
@ConfigurationProperties("primos")
public record PrimosProperties(
        String applicationName,
        int nodeId,
        Path stateDirectory,
        String bindAddress,
        @DefaultValue("0") int bindPort,
        @DefaultValue List<PrimosProperties.PeerProperties> peers,
        PrimosProperties.PeerProperties witness,
        String secret,
        String macAlgorithm,
        @DefaultValue TimingsProperties timings,
        @DefaultValue("true") boolean voteLogging,
        @DefaultValue("context") NodeLifecycle lifecycle,
        @DefaultValue LeaderLifecycle leaderLifecycle) {

    /**
     * When the node joins the cluster.
     *
     * <p>Joining and being <em>ready to lead</em> are not the same event, and tying the first to
     * context refresh assumes they are.
     */
    public enum NodeLifecycle {

        /**
         * The node starts as the context finishes refreshing, and stops with it. The default, and
         * right for an application with nothing to do before it can safely lead.
         */
        CONTEXT,

        /**
         * The node bean is built and <em>not</em> started; the application calls
         * {@link org.lolaf.primos.PrimosNode#start()} when it is genuinely ready — after a cache
         * warm, a migration, a first sync.
         *
         * <p>Stopping is not symmetric: Spring still stops the node on context close, because that
         * is what sends the handover and the departure announcement. Leaving that to an application
         * that must remember to call {@code stop()} would trade the most valuable thing this
         * library does at shutdown for a symmetry nobody asked for.
         */
        MANUAL
    }

    /**
     * Which timing profile the whole cluster runs, witness included. A profile must be uniform
     * across a cluster: mixing them makes one member measurably more reluctant to agree to the
     * failover its vote is needed for, and nothing breaks loudly when it happens.
     */
    public enum Profile { LAN, WAN }

    /**
     * The intervals that govern elections and failure detection: <strong>either</strong> a named
     * profile <strong>or</strong> the values spelled out, never both.
     *
     * <p>Refusing the mixture is deliberate. A profile is a set of values chosen to hold Raft's
     * governing inequality together, so "{@code wan} but with a different heartbeat" is not a
     * profile any more — it is a custom set whose author has not seen the other four numbers, and
     * the failures that follow are slow rather than loud. Naming all five is a small price for
     * knowing what you configured.
     *
     * <p>Every value is nullable, because the question this record has to answer is not "what is
     * it" but "<em>was it set</em>". Anything left out of a custom set falls back to the
     * {@link org.lolaf.primos.Timings} builder's own default, and the relationships between them
     * are validated there — one place, with the messages already written.
     *
     * @param profile            {@code lan} or {@code wan}
     * @param heartbeatInterval  how often a leader asserts itself
     * @param electionTimeoutMin the low end of the randomised election timeout. Set with its pair
     * @param electionTimeoutMax the high end. Set with its pair
     * @param probeInterval      how often every node probes every peer
     * @param advertisementInterval how often a node restates its own intervals, so a mismatch with
     *                           a peer is reported. Never shorter than {@code probeInterval}
     * @param failureThreshold   consecutive missed probes before a peer is reported down
     */
    public record TimingsProperties(
            Profile profile,
            Duration heartbeatInterval,
            Duration electionTimeoutMin,
            Duration electionTimeoutMax,
            Duration probeInterval,
            Duration advertisementInterval,
            Integer failureThreshold) {

        /**
         * The {@code primos.timings.*} keys that were actually set, spelled as an operator wrote
         * them.
         *
         * <p>Names rather than a yes-or-no answer, because the caller needs both: whether any were
         * set, and <em>which</em>, so that refusing a profile beside them can quote them back. In a
         * Spring application the values may have come from a YAML file, a profile-specific one, an
         * environment variable or the command line, and "you also set some intervals" leaves the
         * operator to search all four.
         *
         * <p>Package-private: this exists for {@link PrimosAutoConfiguration}, not for callers.
         */
        List<String> explicitKeys() {
            List<String> keys = new ArrayList<>(5);
            addIfSet(keys, "heartbeat-interval", heartbeatInterval);
            addIfSet(keys, "election-timeout-min", electionTimeoutMin);
            addIfSet(keys, "election-timeout-max", electionTimeoutMax);
            addIfSet(keys, "probe-interval", probeInterval);
            addIfSet(keys, "advertisement-interval", advertisementInterval);
            addIfSet(keys, "failure-threshold", failureThreshold);
            return keys;
        }

        private static void addIfSet(List<String> keys, String key, Object value) {
            if (value != null) {
                keys.add(key);
            }
        }
    }

    /**
     * One member of the cluster other than this node.
     *
     * @param id   its node id
     * @param name a human-readable name for logs; defaults to the id
     * @param host its hostname or address, resolved once when the transport is built
     * @param port its port
     */
    public record PeerProperties(int id, String name, String host, int port) {
    }

    /**
     * Whether leadership starts and stops beans, and which ones.
     *
     * @param enabled off by default. Leadership-gated beans look like a solution to the
     *                stale-leader window and are not one; see the module's documentation.
     * @param scope   which beans leadership governs
     * @param beans   explicit bean names, which win over {@link LeaderOnly} when set
     */
    public record LeaderLifecycle(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("beans") Scope scope,
            @DefaultValue List<String> beans) {

        /**
         * Which beans leadership governs.
         */
        public enum Scope {

            /**
             * The beans named in {@code primos.leader-lifecycle.beans}, or failing that every
             * {@link org.springframework.context.Lifecycle} bean marked {@link LeaderOnly}.
             */
            BEANS,

            /**
             * Every {@link org.springframework.context.Lifecycle} bean in the context, primos' own
             * excepted.
             *
             * <p>A legitimate choice for an application whose whole reason to exist is the leader
             * work, and a loud one: it takes the web server and the actuator down with it, so
             * losing leadership blinds the monitoring at precisely the moment somebody is reading
             * it. Selecting it logs a warning naming what goes down.
             */
            CONTEXT
        }
    }
}
