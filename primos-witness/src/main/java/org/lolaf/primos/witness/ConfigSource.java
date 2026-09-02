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

/**
 * Where a served application's configuration comes from.
 *
 * <p>One axis, deliberately, rather than one setting per thing that could be sourced either way.
 * Secrets are the exception that cannot be on it: a secret can never arrive over the wire, because
 * it is what authenticates the datagram that would carry it, and {@code secret.algorithm} is local
 * policy for the same reason the wire carries no algorithm name — a datagram naming its own
 * algorithm lets an attacker pick the weakest one both ends accept. So the {@code applications/}
 * directory is read under either value; what changes is whether it is the served list or only a
 * keyring.
 */
public enum ConfigSource {

    /**
     * Applications and their timings come from the configuration directory, and nothing else.
     *
     * <p>The default, and the only value under which no configuration arrives over the network.
     * This is the deployment whose operator has a hard constraint against automatic change.
     */
    FILE,

    /**
     * Applications are created, and their timings taken, from the config messages their nodes send.
     *
     * <p>Much less operational burden — a new cluster just points at the witness, and retuning it
     * needs no change here — at the cost of the failure that made {@link #FILE} the default. Nodes
     * configured {@code orders} and {@code ordres} drop each other's datagrams, so each sees the
     * other as unreachable; each then asks the witness, which creates <em>two</em> buckets and
     * grants a vote from each. Both reach a majority. Two leaders, from one transposed letter,
     * with the cross-talk protection hiding the cause. Names that look like near misses are warned
     * about individually, and the count of cluster-created applications is capped.
     */
    CLUSTER;

    public boolean isCluster() {
        return this == CLUSTER;
    }
}
