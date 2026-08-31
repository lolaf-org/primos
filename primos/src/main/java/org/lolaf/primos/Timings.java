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

import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Objects;

/**
 * The intervals that govern elections and failure detection.
 *
 * <p>The relationships matter more than the values. Raft's governing inequality is
 * {@code broadcastTime << electionTimeout << MTBF}: the election timeout must be comfortably
 * above a network round trip, or nodes will unseat a healthy leader whenever a packet is late.
 * Every rule below is enforced at construction rather than left to be discovered in production.
 *
 * <p>Use {@link #lan()} for nodes on one network and {@link #wan()} when a witness sits on
 * another, where a fifty-millisecond round trip makes the LAN defaults marginal.
 *
 * <p><strong>A node runs three independent periodic mechanisms, and they are easy to confuse.</strong>
 * They differ in who sends, what is at stake, and therefore in rate — three orders of magnitude
 * between the fastest and the slowest, and the reason is entirely in the last column:
 *
 * <table border="1">
 *   <caption>The three periodic mechanisms</caption>
 *   <tr><th>interval</th><th>sent by</th><th>what it is for</th><th>cost of losing one</th></tr>
 *   <tr>
 *     <td>{@link #heartbeatInterval()} (50ms)</td>
 *     <td>the leader only</td>
 *     <td>stopping followers campaigning</td>
 *     <td><strong>an election</strong> — a real failover</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #probeInterval()} (1s)</td>
 *     <td>every node, to every peer</td>
 *     <td>is that peer reachable?</td>
 *     <td>a peer looks down after {@link #failureThreshold()} of them</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #advertisementInterval()} (30s)</td>
 *     <td>every node, to every peer</td>
 *     <td>do we agree on configuration?</td>
 *     <td>a log line, later or not at all</td>
 *   </tr>
 * </table>
 *
 * <p>Put shortly: a missed heartbeat costs a leader, a missed advertisement costs a log line. That
 * is the whole reason one is fifty milliseconds and the other is thirty seconds.
 */
@Value
@Builder(builderClassName = "Builder", buildMethodName = "buildUnvalidated")
public class Timings {

    /**
     * The election timeout must be at least this many heartbeat intervals. A leader has to miss
     * several heartbeats before anyone concludes it is gone, or ordinary jitter causes elections.
     */
    public static final int MIN_ELECTION_TIMEOUT_HEARTBEATS = 5;

    /**
     * Probes must not be rarer than this. Symmetric probing is what keeps a NAT or firewall
     * pinhole open to a witness on another network, and those commonly expire after thirty
     * seconds; probing less often than this risks a hole closing between probes, which reads as a
     * peer flapping rather than as a network setting.
     */
    public static final Duration MAX_PROBE_INTERVAL = Duration.ofSeconds(15);
    /**
     * How often a leader asserts itself, by sending an empty {@code AppendEntries} to every peer.
     *
     * <p>Sent by <strong>the leader alone</strong>, and its only job is to say <em>I am still here,
     * do not campaign</em>: every one that arrives resets a follower's election timer. Miss enough
     * of them and a follower concludes the leader is gone and stands for election, which is why
     * this is the fastest thing in the system and why {@link #electionTimeoutMin()} must be at
     * least {@value #MIN_ELECTION_TIMEOUT_HEARTBEATS} times it.
     *
     * <p>Not to be confused with {@link #probeInterval()}, which measures reachability, or
     * {@link #advertisementInterval()}, which reports configuration. See the table on this class.
     */
    @lombok.Builder.Default
    Duration heartbeatInterval = Duration.ofMillis(50);

    /**
     * The low end of the range an election timeout is drawn from, afresh on every reset.
     */
    @lombok.Builder.Default
    Duration electionTimeoutMin = Duration.ofMillis(300);

    /**
     * The high end of that range. The width between the two is what stops nodes campaigning in
     * lockstep, so it is never zero.
     */
    @lombok.Builder.Default
    Duration electionTimeoutMax = Duration.ofMillis(600);

    /**
     * How often <strong>every</strong> node probes <strong>every</strong> peer, with a
     * {@code Ping} that is answered with the sequence number it carried.
     *
     * <p>This is reachability — what turns into {@code onPeerUp} and {@code onPeerDown} after
     * {@link #failureThreshold()} consecutive misses — and it is deliberately far slower than the
     * heartbeat, because losing one costs nothing immediately. Every node probes rather than the
     * leader reporting who it can see, so the answer still exists during an election, when nobody
     * is leading and somebody is asking who is up.
     *
     * <p>Capped at {@link #MAX_PROBE_INTERVAL}: symmetric probing is what holds a NAT or firewall
     * pinhole open to a witness on another network.
     */
    @lombok.Builder.Default
    Duration probeInterval = Duration.ofSeconds(1);

    /**
     * How often every node restates its own intervals to its peers, so that a disagreement is
     * reported.
     *
     * <p>Purely diagnostic, and the only one of the three that <strong>changes no decision</strong>:
     * a node that discovers a mismatch logs it and behaves exactly as before. Acting on one would
     * turn an ordinary rolling timing change into a partition.
     *
     * <p>Slower than everything else here by a wide margin, because the values it carries
     * <em>cannot change while a process runs</em>. In principle one datagram would do; it repeats
     * only because this is UDP and a single datagram may never arrive — and the case where it is
     * dropped is exactly the misconfigured cluster it exists to report. The first goes out one
     * {@link #probeInterval()} after start, so a mismatch surfaces within a second or two whatever
     * this is set to: what this governs is how quickly a <em>changed</em> configuration is noticed,
     * which is a rolling deploy rather than a startup.
     *
     * <p>Never shorter than {@link #probeInterval()}. Restating unchanging values more often than a
     * node checks whether its peers are alive at all is pure traffic.
     */
    @lombok.Builder.Default
    Duration advertisementInterval = Duration.ofSeconds(30);

    /**
     * How many consecutive probes a peer may miss before it is reported down.
     *
     * <p>Multiplied by {@link #probeInterval()}, this is how long a peer may be silent before
     * anyone says so — and on the JVM that product has to clear the worst stop-the-world pause,
     * because a collecting process is indistinguishable from a dead one.
     */
    @lombok.Builder.Default
    int failureThreshold = 3;

    /**
     * Defaults for a cluster on one network: 50ms heartbeat, election timeout drawn from
     * 300-600ms, probes every second, three missed probes to call a peer down.
     */
    public static Timings lan() {
        return builder().build();
    }

    /**
     * Defaults for a cluster whose witness is on another network: 200ms heartbeat, election
     * timeout drawn from 1.5-3s, probes every two seconds.
     */
    public static Timings wan() {
        return builder()
                .heartbeatInterval(Duration.ofMillis(200))
                .electionTimeoutMin(Duration.ofMillis(1_500))
                .electionTimeoutMax(Duration.ofMillis(3_000))
                .probeInterval(Duration.ofSeconds(2))
                .build();
    }

    private static void requirePositive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(what + " must be positive, but was " + value);
        }
    }

    /**
     * The width of the range an election timeout is drawn from. It is never zero: nodes that time
     * out together campaign together, split the vote and repeat, so randomisation is what makes
     * elections terminate at all.
     */
    public Duration electionTimeoutSpread() {
        return electionTimeoutMax.minus(electionTimeoutMin);
    }

    /**
     * Every rule, checked before a {@link Timings} escapes its builder.
     *
     * <p>Called from {@link Builder#build()} rather than from a constructor, because the
     * constructor is Lombok's now — which is also why the generated build method is named
     * {@code buildUnvalidated} and wrapped: it is the only seam where this can run without
     * hand-writing the {@code @Builder.Default} machinery.
     */
    private Timings validate() {
        requirePositive(heartbeatInterval, "heartbeatInterval");
        requirePositive(electionTimeoutMin, "electionTimeoutMin");
        requirePositive(electionTimeoutMax, "electionTimeoutMax");
        requirePositive(probeInterval, "probeInterval");
        requirePositive(advertisementInterval, "advertisementInterval");
        if (failureThreshold < 1) {
            throw new IllegalArgumentException(
                    "failureThreshold must be at least 1, but was " + failureThreshold);
        }
        Duration floor = heartbeatInterval.multipliedBy(MIN_ELECTION_TIMEOUT_HEARTBEATS);
        if (electionTimeoutMin.compareTo(floor) < 0) {
            throw new IllegalArgumentException(
                    "electionTimeoutMin must be at least " + MIN_ELECTION_TIMEOUT_HEARTBEATS
                            + " heartbeat intervals (" + floor.toMillis() + "ms) but was "
                            + electionTimeoutMin.toMillis() + "ms: a leader must be allowed to miss"
                            + " several heartbeats before anyone campaigns");
        }
        if (electionTimeoutMin.compareTo(electionTimeoutMax) > 0) {
            throw new IllegalArgumentException(
                    "electionTimeoutMin (" + electionTimeoutMin.toMillis()
                            + "ms) is above electionTimeoutMax (" + electionTimeoutMax.toMillis()
                            + "ms), so no timeout could ever be drawn from that range. Setting one"
                            + " bound on its own leaves the other at its default, which is the"
                            + " usual way to arrive here — set both.");
        }
        // Only equality can still reach this, the inverted case having been rejected above. The
        // comparison stays <= rather than == so that deleting the check above would fail loudly
        // rather than let an inverted range through.
        if (electionTimeoutMax.compareTo(electionTimeoutMin) <= 0) {
            throw new IllegalArgumentException(
                    "electionTimeoutMax (" + electionTimeoutMax.toMillis()
                            + "ms) must be greater than electionTimeoutMin ("
                            + electionTimeoutMin.toMillis() + "ms): a range of zero width means"
                            + " every node draws the same timeout and campaigns in lockstep");
        }
        if (electionTimeoutSpread().compareTo(heartbeatInterval) < 0) {
            throw new IllegalArgumentException(
                    "the election timeout range must span at least one heartbeat interval ("
                            + heartbeatInterval.toMillis() + "ms) but spans only "
                            + electionTimeoutSpread().toMillis()
                            + "ms: too narrow a range lets nodes campaign in lockstep and split the"
                            + " vote repeatedly");
        }
        if (advertisementInterval.compareTo(probeInterval) < 0) {
            throw new IllegalArgumentException(
                    "advertisementInterval (" + advertisementInterval.toMillis()
                            + "ms) must not be shorter than probeInterval ("
                            + probeInterval.toMillis() + "ms): the intervals a node advertises"
                            + " cannot change while it runs, so restating them more often than it"
                            + " checks whether its peers are even alive is pure traffic");
        }
        if (probeInterval.compareTo(MAX_PROBE_INTERVAL) > 0) {
            throw new IllegalArgumentException(
                    "probeInterval must not exceed " + MAX_PROBE_INTERVAL.getSeconds()
                            + "s but was " + probeInterval.toMillis()
                            + "ms: probing less often risks a NAT pinhole closing between probes");
        }
        return this;
    }

    public static class Builder {

        public Timings build() {
            return buildUnvalidated().validate();
        }
    }
}
