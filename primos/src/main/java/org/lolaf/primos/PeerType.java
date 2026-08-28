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
 * What kind of member a peer is. Both kinds are full voters; they differ in whether they run the
 * application and whether they may lead.
 */
public enum PeerType {

    /** Another instance of the clustered application. Votes, and may become leader. */
    APPLICATION,

    /**
     * A witness: a voter that never campaigns and so never leads.
     *
     * <p>It exists to give a two-node pair an odd number of voters, so a partition has a winner
     * and a loser rather than two winners. It does not run the application, so work must never be
     * forwarded to it, and a "cluster members" display should not list it.
     */
    WITNESS;

    /** Whether this is the {@link #WITNESS} type. */
    public boolean isWitness() {
        return this == WITNESS;
    }
}
