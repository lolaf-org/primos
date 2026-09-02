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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts everything a node reports, for tests to assert on.
 *
 * <p>This is what replaced {@code PrimosStats}: the node stopped keeping counters that only tests
 * read, and the tests keep their own. Published through the test-jar so the witness's tests can use
 * it too.
 *
 * <p>Fields are atomic because a real node reports from its own thread while a test asserts from
 * the JUnit one. That is a test's problem and not the SPI's — a production listener is called only
 * from the node thread and needs nothing of the sort.
 */
public final class RecordingStats implements PrimosStatsListener {

    public final AtomicLong electionsWon = new AtomicLong();
    public final AtomicLong electionsLost = new AtomicLong();
    public final AtomicLong leadersKnown = new AtomicLong();
    public final AtomicLong leadershipsRelinquished = new AtomicLong();
    public final AtomicLong handoversSent = new AtomicLong();
    public final AtomicLong handoversAccepted = new AtomicLong();
    public final AtomicLong departuresObserved = new AtomicLong();
    public final AtomicLong timingMismatches = new AtomicLong();
    public final AtomicLong unusableNodeConfigs = new AtomicLong();
    public final AtomicLong peersForgotten = new AtomicLong();

    /** Every probe round trip, in order — the samples a percentile would be built from. */
    public final List<Long> probeRoundTrips = new ArrayList<>();
    public final List<Long> campaignsWon = new ArrayList<>();
    public final List<Long> campaignsLost = new ArrayList<>();
    public final List<Long> leaderlessPeriods = new ArrayList<>();

    private final Map<DropReason, AtomicLong> drops = new EnumMap<>(DropReason.class);

    public RecordingStats() {
        for (DropReason reason : DropReason.values()) {
            drops.put(reason, new AtomicLong());
        }
    }

    @Override
    public void onElectionWon(long term, long campaignNanos) {
        electionsWon.incrementAndGet();
        synchronized (campaignsWon) {
            campaignsWon.add(campaignNanos);
        }
    }

    @Override
    public void onElectionLost(long term, long campaignNanos) {
        electionsLost.incrementAndGet();
        synchronized (campaignsLost) {
            campaignsLost.add(campaignNanos);
        }
    }

    @Override
    public void onLeaderKnown(long term, long leaderlessNanos) {
        leadersKnown.incrementAndGet();
        synchronized (leaderlessPeriods) {
            leaderlessPeriods.add(leaderlessNanos);
        }
    }

    @Override
    public void onLeadershipRelinquished(long term) {
        leadershipsRelinquished.incrementAndGet();
    }

    @Override
    public void onHandoverSent(int toPeerId) {
        handoversSent.incrementAndGet();
    }

    @Override
    public void onHandoverAccepted(long term) {
        handoversAccepted.incrementAndGet();
    }

    @Override
    public void onDepartureObserved(int peerId) {
        departuresObserved.incrementAndGet();
    }

    @Override
    public void onProbeRoundTrip(int peerId, long nanos) {
        synchronized (probeRoundTrips) {
            probeRoundTrips.add(nanos);
        }
    }

    @Override
    public void onDatagramDropped(DropReason reason) {
        drops.get(reason).incrementAndGet();
    }

    @Override
    public void onTimingMismatch(int peerId) {
        timingMismatches.incrementAndGet();
    }

    @Override
    public void onUnusableNodeConfig(int peerId) {
        unusableNodeConfigs.incrementAndGet();
    }

    @Override
    public void onPeerForgotten(int peerId) {
        peersForgotten.incrementAndGet();
    }

    public long drops(DropReason reason) {
        return drops.get(reason).get();
    }

    public long totalDrops() {
        long total = 0;
        for (AtomicLong count : drops.values()) {
            total += count.get();
        }
        return total;
    }
}
