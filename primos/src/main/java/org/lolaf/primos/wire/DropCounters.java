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
package org.lolaf.primos.wire;

import org.lolaf.primos.DropReason;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Counts discarded datagrams, one counter per reason.
 *
 * <p>A cluster that is silently dropping everything has to be diagnosable from a metric rather
 * than from a packet capture, and the reason is what tells an operator which problem they have: a
 * stream of {@code WRONG_APPLICATION} is a neighbouring cluster or a typo, while {@code BAD_TAG}
 * is a wrong secret or someone forging traffic.
 *
 * <p>Backed by a plain array so recording costs nothing on the receive path. Owned by one node
 * thread and not thread-safe; {@link #snapshot()} is the way out.
 */
public final class DropCounters {

    private static final DropReason[] REASONS = DropReason.values();

    private final long[] counts = new long[REASONS.length];

    /**
     * Records one discarded datagram.
     */
    public void record(DropReason reason) {
        counts[reason.ordinal()]++;
    }

    /**
     * How many datagrams were discarded for one reason.
     */
    public long count(DropReason reason) {
        return counts[reason.ordinal()];
    }

    /**
     * Every datagram discarded, for any reason.
     */
    public long total() {
        long total = 0;
        for (long count : counts) {
            total += count;
        }
        return total;
    }

    /**
     * An immutable copy, for {@link org.lolaf.primos.PrimosStats}.
     */
    public Map<DropReason, Long> snapshot() {
        EnumMap<DropReason, Long> snapshot = new EnumMap<>(DropReason.class);
        for (DropReason reason : REASONS) {
            snapshot.put(reason, counts[reason.ordinal()]);
        }
        return Collections.unmodifiableMap(snapshot);
    }
}
