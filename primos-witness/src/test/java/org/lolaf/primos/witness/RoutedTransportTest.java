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
package org.lolaf.primos.witness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.SendDropReason;

/**
 * A reply the socket had no room for is a reply that never left, and is reported like one.
 */
class RoutedTransportTest {

    private static final int REPLY_BYTES = 16;
    private static final int NODE = 1;

    @Test
    void a_reply_the_socket_had_no_room_for_is_reported() throws IOException {
        assertThat(outcomeWhenChannelAccepts(0)).isEqualTo(SendDropReason.SOCKET_ERROR);
    }

    @Test
    void a_reply_the_socket_took_whole_reports_nothing() throws IOException {
        assertThat(outcomeWhenChannelAccepts(REPLY_BYTES)).isNull();
    }

    private static SendDropReason outcomeWhenChannelAccepts(int bytes) throws IOException {
        DatagramChannel channel = mock(DatagramChannel.class);
        when(channel.send(any(), any())).thenReturn(bytes);
        AtomicReference<SendDropReason> outcome = new AtomicReference<>();
        try (RoutedTransport transport = new RoutedTransport("orders", channel, mock(Selector.class))) {
            transport.offer(ByteBuffer.allocate(REPLY_BYTES), new InetSocketAddress("127.0.0.1", 7000));
            transport.poll(0L, datagram -> outcome.set(transport.send(NODE, ByteBuffer.allocate(REPLY_BYTES))));
            return outcome.get();
        }
    }
}
