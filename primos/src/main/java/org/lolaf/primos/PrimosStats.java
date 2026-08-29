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

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

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
@Getter
@EqualsAndHashCode
@ToString
public final class PrimosStats {

    private final Role role;
    private final long currentTerm;
    private final Duration timeInRole;
    private final long electionsStarted;
    private final long electionsWon;
    private final long handoversSent;
    private final long handoversAccepted;
    private final Map<DropReason, Long> drops;
    private final Map<Integer, PeerStats> peers;

    public PrimosStats(Role role,
                       long currentTerm,
                       Duration timeInRole,
                       long electionsStarted,
                       long electionsWon,
                       long handoversSent,
                       long handoversAccepted,
                       Map<DropReason, Long> drops,
                       Map<Integer, PeerStats> peers) {
        this.role = Objects.requireNonNull(role, "role");
        this.currentTerm = currentTerm;
        this.timeInRole = Objects.requireNonNull(timeInRole, "timeInRole");
        this.electionsStarted = electionsStarted;
        this.electionsWon = electionsWon;
        this.handoversSent = handoversSent;
        this.handoversAccepted = handoversAccepted;

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
     * How many times this node handed leadership on as it shut down.
     *
     * <p>Worth watching across a rolling deploy: the difference between this and the number of
     * times this node was stopped while leading is the number of deploys that cost the cluster a
     * full election timeout with nobody in charge.
     */
    public long handoversSent() {
        return handoversSent;
    }

    /** How many times this node campaigned because a departing leader handed over to it. */
    public long handoversAccepted() {
        return handoversAccepted;
    }

    /** How many datagrams were discarded for one reason. Never null; zero if it never happened. */
    public long drops(DropReason reason) {
        return drops.getOrDefault(reason, 0L);
    }

    /** Every datagram discarded, for any reason. */
    public long totalDrops() {
        return drops.values().stream().mapToLong(Long::longValue).sum();
    }
}
