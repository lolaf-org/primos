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

import lombok.Value;

import java.time.Duration;
import java.util.*;

/**
 * An immutable snapshot of what a node has been doing.
 *
 * <p>This is the whole observability seam. primos depends on nothing but SLF4J, so it cannot
 * export metrics itself; it hands you a value object instead and you wire it to whatever you
 * already run. Without this the counters below would exist and be unreachable.
 *
 * <p>The drop counters are the ones worth watching. A cluster silently dropping every datagram
 * should be diagnosable from a metric rather than a packet capture, and the reason distinguishes a
 * misconfigured neighbour from a wrong secret.
 */
@Value
public class PrimosStats {

    Role role;
    long currentTerm;
    Duration timeInRole;
    long electionsStarted;
    long electionsWon;
    /**
     * How many times this node handed leadership on as it shut down.
     *
     * <p>Worth watching across a rolling deploy: the difference between this and the number of
     * times this node was stopped while leading is the number of deploys that cost the cluster a
     * full election timeout with nobody in charge.
     */
    long handoversSent;
    /**
     * How many times this node campaigned because a departing leader handed over to it.
     */
    long handoversAccepted;
    /**
     * How many peers have told this node they were shutting down.
     *
     * <p>Read against the peer-down events: departures that were announced are orderly shutdowns,
     * and the rest were inferred from silence — a crash, a partition, or a garbage collection long
     * enough to look like one. The two have very different meanings on a dashboard.
     */
    long departuresObserved;

    /**
     * How many times a peer has been seen advertising intervals that differ from this node's.
     *
     * <p>Counts transitions into disagreement, not datagrams: a peer that stays misconfigured is
     * counted once, and counted again only if it agrees and then diverges afresh. Anything above
     * zero means somebody's failover is slower than they think, since a node with a longer
     * {@code electionTimeoutMin} is proportionally more reluctant to grant the pre-vote a failover
     * waits on. Nothing is unsafe; nothing is corrupted; it is simply slow in a way that is
     * invisible from either side, which is the whole reason this is counted at all.
     */
    long timingMismatches;

    Map<DropReason, Long> drops;
    Map<Integer, PeerStats> peers;

    public PrimosStats(Role role,
                       long currentTerm,
                       Duration timeInRole,
                       long electionsStarted,
                       long electionsWon,
                       long handoversSent,
                       long handoversAccepted,
                       long departuresObserved,
                       long timingMismatches,
                       Map<DropReason, Long> drops,
                       Map<Integer, PeerStats> peers) {
        this.role = Objects.requireNonNull(role, "role");
        this.currentTerm = currentTerm;
        this.timeInRole = Objects.requireNonNull(timeInRole, "timeInRole");
        this.electionsStarted = electionsStarted;
        this.electionsWon = electionsWon;
        this.handoversSent = handoversSent;
        this.handoversAccepted = handoversAccepted;
        this.departuresObserved = departuresObserved;
        this.timingMismatches = timingMismatches;

        EnumMap<DropReason, Long> allReasons = new EnumMap<>(DropReason.class);
        for (DropReason reason : DropReason.values()) {
            allReasons.put(reason, 0L);
        }
        if (drops != null) {
            allReasons.putAll(drops);
        }
        this.drops = Collections.unmodifiableMap(allReasons);
        this.peers = Collections.unmodifiableMap(
                new LinkedHashMap<>(peers == null ? Collections.emptyMap() : peers));
    }

    /**
     * How many datagrams were discarded for one reason. Never null; zero if it never happened.
     */
    public long drops(DropReason reason) {
        return drops.getOrDefault(reason, 0L);
    }

    /**
     * Every datagram discarded, for any reason.
     */
    public long totalDrops() {
        return drops.values().stream().mapToLong(Long::longValue).sum();
    }
}
