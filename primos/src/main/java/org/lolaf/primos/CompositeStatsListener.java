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

/**
 * Fans one event out to several sinks, for {@link PrimosStatsListener#of}.
 *
 * <p>An array walked by index rather than a collection walked by iterator, because these methods
 * run on the node thread and one of them runs per dropped datagram: an iterator per event is
 * exactly the allocation this whole SPI is shaped to avoid.
 */
final class CompositeStatsListener implements PrimosStatsListener {

    private final PrimosStatsListener[] sinks;

    CompositeStatsListener(PrimosStatsListener[] sinks) {
        this.sinks = sinks;
    }

    @Override
    public void onElectionWon(long term, long campaignNanos) {
        for (PrimosStatsListener sink : sinks) {
            sink.onElectionWon(term, campaignNanos);
        }
    }

    @Override
    public void onElectionLost(long term, long campaignNanos) {
        for (PrimosStatsListener sink : sinks) {
            sink.onElectionLost(term, campaignNanos);
        }
    }

    @Override
    public void onLeaderKnown(long term, long leaderlessNanos) {
        for (PrimosStatsListener sink : sinks) {
            sink.onLeaderKnown(term, leaderlessNanos);
        }
    }

    @Override
    public void onLeadershipRelinquished(long term) {
        for (PrimosStatsListener sink : sinks) {
            sink.onLeadershipRelinquished(term);
        }
    }

    @Override
    public void onHandoverSent(int toPeerId) {
        for (PrimosStatsListener sink : sinks) {
            sink.onHandoverSent(toPeerId);
        }
    }

    @Override
    public void onHandoverAccepted(long term) {
        for (PrimosStatsListener sink : sinks) {
            sink.onHandoverAccepted(term);
        }
    }

    @Override
    public void onDepartureObserved(int peerId) {
        for (PrimosStatsListener sink : sinks) {
            sink.onDepartureObserved(peerId);
        }
    }

    @Override
    public void onProbeRoundTrip(int peerId, long nanos) {
        for (PrimosStatsListener sink : sinks) {
            sink.onProbeRoundTrip(peerId, nanos);
        }
    }

    @Override
    public void onDatagramDropped(DropReason reason) {
        for (PrimosStatsListener sink : sinks) {
            sink.onDatagramDropped(reason);
        }
    }

    @Override
    public void onTimingMismatch(int peerId) {
        for (PrimosStatsListener sink : sinks) {
            sink.onTimingMismatch(peerId);
        }
    }

    @Override
    public void onUnusableNodeConfig(int peerId) {
        for (PrimosStatsListener sink : sinks) {
            sink.onUnusableNodeConfig(peerId);
        }
    }

    @Override
    public void onPeerForgotten(int peerId) {
        for (PrimosStatsListener sink : sinks) {
            sink.onPeerForgotten(peerId);
        }
    }
}
