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

import org.junit.jupiter.api.Test;

/**
 * A reply the socket had no room for is a reply that never left, and is counted like one.
 */
class RoutedTransportTest {

    private static final int REPLY_BYTES = 16;
    private static final int NODE = 1;

    @Test
    void a_reply_the_socket_had_no_room_for_is_counted() throws IOException {
        assertThat(sendsDroppedWhenChannelAccepts(0)).isEqualTo(1);
    }

    @Test
    void a_reply_the_socket_took_whole_is_not_counted() throws IOException {
        assertThat(sendsDroppedWhenChannelAccepts(REPLY_BYTES)).isZero();
    }

    private static long sendsDroppedWhenChannelAccepts(int bytes) throws IOException {
        DatagramChannel channel = mock(DatagramChannel.class);
        when(channel.send(any(), any())).thenReturn(bytes);
        try (RoutedTransport transport = new RoutedTransport("orders", channel, mock(Selector.class))) {
            transport.offer(ByteBuffer.allocate(REPLY_BYTES), new InetSocketAddress("127.0.0.1", 7000));
            transport.poll(0L, datagram -> transport.send(NODE, ByteBuffer.allocate(REPLY_BYTES)));
            return transport.sendsDropped();
        }
    }
}
