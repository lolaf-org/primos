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
package org.lolaf.primos.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.DropReason;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosStatsListener;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * A metrics binder runs on the node thread, on paths that turn at heartbeat rate and, for drops,
 * per datagram. {@code LoopAllocationTest} in the library pins a node's steady state at zero bytes
 * a turn with a counting listener attached; this is the other end of that claim — that the real
 * listener is the counting one.
 *
 * <p>It is why every meter is resolved in the constructor and none is looked up by name or tag when
 * an event arrives: {@code registry.counter(name, tags)} allocates a varargs array and a
 * {@code Tags} on every call, on the one path whose rate an attacker chooses.
 */
class MetricsAllocationTest {

    private static final int PEER = 201;
    private static final DropReason REASON = DropReason.BAD_TAG;

    @Test
    void recording_an_event_allocates_nothing_the_no_op_sink_does_not() {
        long withoutMeters = bytesPerEvent(PrimosStatsListener.NONE);
        long withMeters = bytesPerEvent(binder());

        // Checked against a deliberately broken binder rather than assumed: resolving the counter
        // by name and tags inside the callback, which is the idiomatic Micrometer thing to write,
        // measures 264 bytes an event and fails this. As built it is 2.9KB per 20,000 events, so
        // the delta rounds to nothing.
        assertThat(withMeters - withoutMeters)
                .as("the delta, not the total: the claim is that binding meters costs nothing per"
                        + " event, and a resolved Counter and Timer are the whole of the work")
                .isLessThan(8L);
    }

    /**
     * The negative pair, and the library's own allocation test needed exactly this: its first
     * version measured a listener that was never called, so it weighed nothing and passed.
     */
    @Test
    void the_meters_are_actually_moving() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PrimosMetrics metrics = PrimosMetrics.create(registry, cluster());

        drive(metrics, 10);

        assertThat(registry.get("primos.datagrams.dropped")
                .tag("reason", REASON.name().toLowerCase()).counter().count()).isEqualTo(10.0);
        assertThat(registry.get("primos.probe.rtt").tag("peer", String.valueOf(PEER))
                .timer().count()).isEqualTo(10);
    }

    private PrimosStatsListener binder() {
        return PrimosMetrics.create(new SimpleMeterRegistry(), cluster());
    }

    /**
     * Peer ids above 127 deliberately, as in the library's test: {@code Integer.valueOf} caches
     * −128..127, so a boxing regression is invisible with the ids tests usually use.
     */
    private ClusterConfig cluster() {
        return ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(200)
                .peer(Peer.application(PEER, "host-b", 7000))
                .build();
    }

    private long bytesPerEvent(PrimosStatsListener sink) {
        com.sun.management.ThreadMXBean threads = threadMxBean();
        assumeThat(threads).as("this JVM cannot measure per-thread allocation").isNotNull();

        drive(sink, 2_000);
        long id = Thread.currentThread().getId();
        long before = threads.getThreadAllocatedBytes(id);
        int events = 20_000;
        drive(sink, events);
        return (threads.getThreadAllocatedBytes(id) - before) / events;
    }

    /**
     * The two callbacks that fire most: one per dropped datagram, one per probe reply.
     */
    private void drive(PrimosStatsListener sink, int events) {
        for (int i = 0; i < events; i++) {
            sink.onDatagramDropped(REASON);
            sink.onProbeRoundTrip(PEER, 250_000);
        }
    }

    private static com.sun.management.ThreadMXBean threadMxBean() {
        return ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean
                ? (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean()
                : null;
    }
}
