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
 * Why a peer was reported down, which decides how much to believe it.
 */
public enum PeerDownReason {

    /**
     * It said it was shutting down. A clean stop, known at once and certain: the peer is gone.
     */
    LEFT,

    /**
     * It stopped answering probes. A crash, a partition, or a pause long enough to look like one, such as a garbage
     * collection: the peer may well come back, so act on this only after a grace period of your own.
     */
    UNREACHABLE
}
