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
package org.lolaf.primos.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.ClusterConfig;
import org.lolaf.primos.Peer;
import org.lolaf.primos.PrimosListener;
import org.lolaf.primos.RecordingStats;
import org.lolaf.primos.SendDropReason;
import org.lolaf.primos.store.MemoryStateStore;
import org.lolaf.primos.time.VirtualClock;
import org.lolaf.primos.transport.DatagramHandler;
import org.lolaf.primos.transport.Transport;
import org.lolaf.primos.wire.MessageAuthenticator;

/**
 * A send that never left reaches the stats listener with its reason, which is the only way anybody hears of it.
 */
class SendDropReportingTest {

    private static final Duration PAST_A_PROBE = Duration.ofSeconds(2);

    private final VirtualClock clock = new VirtualClock();
    private final RecordingStats recorded = new RecordingStats();

    @Test
    void a_send_that_never_left_is_reported_with_its_reason() {
        probeOnce(() -> SendDropReason.NO_ADDRESS);

        assertThat(recorded.sendDrops(SendDropReason.NO_ADDRESS)).isPositive();
    }

    @Test
    void a_send_that_left_reports_nothing() {
        probeOnce(() -> null);

        assertThat(recorded.sendsDropped()).isZero();
    }

    @Test
    void a_transport_that_throws_is_reported_and_costs_the_node_nothing() {
        ElectionNode node = probeOnce(() -> {
            throw new IllegalStateException("a transport breaking the no-throw contract");
        });

        assertThat(recorded.sendDrops(SendDropReason.SOCKET_ERROR)).isPositive();
        assertThat(node.isFailed()).isFalse();
    }

    private ElectionNode probeOnce(Supplier<SendDropReason> outcome) {
        ClusterConfig config = ClusterConfig.builder().applicationName("t").nodeId(1)
                .peers(List.of(Peer.application(2, "sim", 7002))).build();
        ElectionNode node = new ElectionNode(config, new MemoryStateStore(), new ScriptedTransport(outcome), clock,
                new Random(1L), MessageAuthenticator.disabled(), new PrimosListener() { }, recorded);
        node.start();
        clock.advance(PAST_A_PROBE);
        node.runOnce(0);
        return node;
    }

    private static final class ScriptedTransport implements Transport {

        private final Supplier<SendDropReason> outcome;

        private ScriptedTransport(Supplier<SendDropReason> outcome) {
            this.outcome = outcome;
        }

        @Override
        public SendDropReason send(int peerId, ByteBuffer datagram) {
            return outcome.get();
        }

        @Override
        public int poll(long timeoutNanos, DatagramHandler handler) {
            return 0;
        }

        @Override
        public void wakeup() {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
