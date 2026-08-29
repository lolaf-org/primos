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

import lombok.Setter;
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
 */
@Value
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

    Duration heartbeatInterval;
    Duration electionTimeoutMin;
    Duration electionTimeoutMax;
    Duration probeInterval;
    int failureThreshold;

    private Timings(Duration heartbeatInterval,
                    Duration electionTimeoutMin,
                    Duration electionTimeoutMax,
                    Duration probeInterval,
                    int failureThreshold) {
        this.heartbeatInterval = requirePositive(heartbeatInterval, "heartbeatInterval");
        this.electionTimeoutMin = requirePositive(electionTimeoutMin, "electionTimeoutMin");
        this.electionTimeoutMax = requirePositive(electionTimeoutMax, "electionTimeoutMax");
        this.probeInterval = requirePositive(probeInterval, "probeInterval");
        this.failureThreshold = failureThreshold;
        validate();
    }

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
                .electionTimeout(Duration.ofMillis(1_500), Duration.ofMillis(3_000))
                .probeInterval(Duration.ofSeconds(2))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Duration requirePositive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(what + " must be positive, but was " + value);
        }
        return value;
    }

    /**
     * The width of the range an election timeout is drawn from. It is never zero: nodes that time
     * out together campaign together, split the vote and repeat, so randomisation is what makes
     * elections terminate at all.
     */
    public Duration electionTimeoutSpread() {
        return electionTimeoutMax.minus(electionTimeoutMin);
    }

    private void validate() {
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
        if (electionTimeoutMax.compareTo(electionTimeoutMin) <= 0) {
            throw new IllegalArgumentException(
                    "electionTimeoutMax (" + electionTimeoutMax.toMillis()
                            + "ms) must be greater than electionTimeoutMin ("
                            + electionTimeoutMin.toMillis() + "ms)");
        }
        if (electionTimeoutSpread().compareTo(heartbeatInterval) < 0) {
            throw new IllegalArgumentException(
                    "the election timeout range must span at least one heartbeat interval ("
                            + heartbeatInterval.toMillis() + "ms) but spans only "
                            + electionTimeoutSpread().toMillis()
                            + "ms: too narrow a range lets nodes campaign in lockstep and split the"
                            + " vote repeatedly");
        }
        if (probeInterval.compareTo(MAX_PROBE_INTERVAL) > 0) {
            throw new IllegalArgumentException(
                    "probeInterval must not exceed " + MAX_PROBE_INTERVAL.getSeconds()
                            + "s but was " + probeInterval.toMillis()
                            + "ms: probing less often risks a NAT pinhole closing between probes");
        }
    }

    /**
     * Builder for {@link Timings}; every value defaults to the {@link Timings#lan()} profile.
     *
     * <p>{@code @Setter} is on the fields that may be set individually, and deliberately not on
     * {@code electionTimeoutMin}/{@code electionTimeoutMax}: those two are only ever set together,
     * through {@link #electionTimeout}, because setting one alone is how an invalid range is
     * built. Annotating per field rather than per class is what keeps that constraint.
     */
    public static final class Builder {

        /**
         * How often a leader asserts itself. Must stay well below the election timeout.
         */
        @Setter
        private Duration heartbeatInterval = Duration.ofMillis(50);

        private Duration electionTimeoutMin = Duration.ofMillis(300);
        private Duration electionTimeoutMax = Duration.ofMillis(600);

        /**
         * How often every node probes every peer. Independent of the heartbeat, and much slower.
         */
        @Setter
        private Duration probeInterval = Duration.ofSeconds(1);

        /**
         * How many consecutive probes a peer may miss before it is reported down.
         */
        @Setter
        private int failureThreshold = 3;

        private Builder() {
        }

        /**
         * The inclusive range each election timeout is drawn from, afresh on every reset.
         */
        public Builder electionTimeout(Duration min, Duration max) {
            this.electionTimeoutMin = min;
            this.electionTimeoutMax = max;
            return this;
        }

        public Timings build() {
            return new Timings(heartbeatInterval, electionTimeoutMin, electionTimeoutMax,
                    probeInterval, failureThreshold);
        }
    }
}
