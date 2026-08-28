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
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** What this node has observed about one peer, as of the moment a snapshot was taken. */
@Getter
@EqualsAndHashCode
@ToString
public final class PeerStats {

    private final int peerId;
    private final PeerType type;
    private final boolean reachable;
    private final Duration lastRoundTrip;
    private final Duration sinceLastHeard;

    public PeerStats(int peerId,
                     PeerType type,
                     boolean reachable,
                     Duration lastRoundTrip,
                     Duration sinceLastHeard) {
        this.peerId = peerId;
        this.type = Objects.requireNonNull(type, "type");
        this.reachable = reachable;
        this.lastRoundTrip = lastRoundTrip;
        this.sinceLastHeard = sinceLastHeard;
    }
}
