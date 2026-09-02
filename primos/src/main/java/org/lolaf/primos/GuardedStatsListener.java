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

import lombok.extern.slf4j.Slf4j;

/**
 * Keeps a throwing stats listener from taking the node down with it.
 *
 * <p>Without this, an exception from a metrics binder would reach {@code runOnce}'s catch-all,
 * which marks the node permanently {@code FAILED} — so a bug in a counter would cost a cluster a
 * voter. A binder must not be able to stop consensus, and it is the one thing here that is not
 * load-bearing.
 *
 * <p>Applied once at the boundary rather than at each call site, so the algorithm calls its sink
 * plainly. The try blocks cost nothing while nothing throws, and a caught exception is logged
 * without arguments beyond the event name — building a message with the values would allocate on
 * the path this whole SPI is shaped to keep allocation-free.
 */
@Slf4j
final class GuardedStatsListener implements PrimosStatsListener {

    private final PrimosStatsListener sink;
    private final String application;
    private final int nodeId;

    GuardedStatsListener(PrimosStatsListener sink, String application, int nodeId) {
        this.sink = sink;
        this.application = application;
        this.nodeId = nodeId;
    }

    /**
     * Wraps only when there is something that could throw. {@link PrimosStatsListener#NONE} cannot,
     * so a node without a listener keeps its single monomorphic call to a method that does nothing.
     */
    static PrimosStatsListener guard(PrimosStatsListener sink, String application, int nodeId) {
        return sink == PrimosStatsListener.NONE
                ? PrimosStatsListener.NONE
                : new GuardedStatsListener(sink, application, nodeId);
    }

    @Override
    public void onElectionWon(long term, long campaignNanos) {
        try {
            sink.onElectionWon(term, campaignNanos);
        } catch (RuntimeException e) {
            failed("onElectionWon", e);
        }
    }

    @Override
    public void onElectionLost(long term, long campaignNanos) {
        try {
            sink.onElectionLost(term, campaignNanos);
        } catch (RuntimeException e) {
            failed("onElectionLost", e);
        }
    }

    @Override
    public void onLeaderKnown(long term, long leaderlessNanos) {
        try {
            sink.onLeaderKnown(term, leaderlessNanos);
        } catch (RuntimeException e) {
            failed("onLeaderKnown", e);
        }
    }

    @Override
    public void onLeadershipRelinquished(long term) {
        try {
            sink.onLeadershipRelinquished(term);
        } catch (RuntimeException e) {
            failed("onLeadershipRelinquished", e);
        }
    }

    @Override
    public void onHandoverSent(int toPeerId) {
        try {
            sink.onHandoverSent(toPeerId);
        } catch (RuntimeException e) {
            failed("onHandoverSent", e);
        }
    }

    @Override
    public void onHandoverAccepted(long term) {
        try {
            sink.onHandoverAccepted(term);
        } catch (RuntimeException e) {
            failed("onHandoverAccepted", e);
        }
    }

    @Override
    public void onDepartureObserved(int peerId) {
        try {
            sink.onDepartureObserved(peerId);
        } catch (RuntimeException e) {
            failed("onDepartureObserved", e);
        }
    }

    @Override
    public void onProbeRoundTrip(int peerId, long nanos) {
        try {
            sink.onProbeRoundTrip(peerId, nanos);
        } catch (RuntimeException e) {
            failed("onProbeRoundTrip", e);
        }
    }

    @Override
    public void onDatagramDropped(DropReason reason) {
        try {
            sink.onDatagramDropped(reason);
        } catch (RuntimeException e) {
            failed("onDatagramDropped", e);
        }
    }

    @Override
    public void onTimingMismatch(int peerId) {
        try {
            sink.onTimingMismatch(peerId);
        } catch (RuntimeException e) {
            failed("onTimingMismatch", e);
        }
    }

    @Override
    public void onUnusableNodeConfig(int peerId) {
        try {
            sink.onUnusableNodeConfig(peerId);
        } catch (RuntimeException e) {
            failed("onUnusableNodeConfig", e);
        }
    }

    @Override
    public void onPeerForgotten(int peerId) {
        try {
            sink.onPeerForgotten(peerId);
        } catch (RuntimeException e) {
            failed("onPeerForgotten", e);
        }
    }

    private void failed(String event, RuntimeException e) {
        log.error("cluster '{}': node {} stats listener threw from {}; the node carries on, but"
                + " that counter is now wrong", application, nodeId, event, e);
    }
}
