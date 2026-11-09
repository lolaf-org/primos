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

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.lolaf.primos.Timings;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;

/**
 * The witness server's own settings, as opposed to the applications it serves.
 *
 * <p>Value equality is not decoration: it is how a reload tells a settings-only change from no
 * change at all.
 */
@Value
@Builder
public class WitnessSettings {

    /**
     * Where to listen. Not hot-reloadable: a change is read and ignored until a restart.
     */
    @NonNull
    @Builder.Default
    InetSocketAddress bindAddress = new InetSocketAddress(7420);

    /**
     * Where per-application term and vote files live. Must be durable local storage.
     */
    @NonNull
    Path stateDirectory;

    /**
     * Where a served application's configuration comes from. See {@link ConfigSource}.
     */
    @NonNull
    @Builder.Default
    ConfigSource configSource = ConfigSource.FILE;

    /**
     * Every key this witness holds, and which applications each may speak for.
     *
     * <p>Witness-wide rather than per application, and that is what makes keys independent of
     * {@link ConfigSource}: a cluster arriving over the network has no file to have had a secret
     * set in, so keys kept per application could offer it only one shared key or none.
     */
    @NonNull
    @Builder.Default
    Keyring keyring = Keyring.empty();

    /**
     * How many applications {@link ConfigSource#CLUSTER} may create, so an open witness cannot be
     * filled up by whoever can reach the port.
     */
    @Builder.Default
    int clusterApplicationLimit = 64;

    /**
     * How often the configuration is checked for change, which is the whole of the reload mechanism.
     * A tick does nothing further unless a file has changed, so this is cheap enough to be seconds.
     */
    @NonNull
    @Builder.Default
    Duration rescanInterval = Duration.ofSeconds(5);

    /**
     * The witness-wide <em>default</em> intervals, not, despite the obvious reading, what any node
     * is run with.
     *
     * <p>Worth stating plainly, because nothing consumes this at serving time and it looks dead:
     * every served node takes its intervals from its own {@link ApplicationConfig}, since a witness
     * serves clusters it does not own and one of them on another network must not decide the
     * intervals for all of them. This value is what an {@code ApplicationConfig} falls back to, at
     * the two places one is built:
     *
     * <ul>
     *   <li>{@link FileConfigStore}, for an application whose file names no intervals of its own:
     *       a tenant that says nothing gets what the witness was told;</li>
     *   <li>auto-registration under {@link ConfigSource#CLUSTER}, where an application created from
     *       a datagram has no file at all. It runs these until its nodes state their own, which
     *       under that setting this witness then follows.</li>
     * </ul>
     *
     * <p>Defaults to {@link Timings#lan()}, matching the library, because a witness whose pre-vote
     * window differs from its cluster's answers questions the cluster is not asking. A witness
     * genuinely on a third network wants {@link Timings#wan()}: a 50ms WAN round trip makes the
     * LAN election timeout marginal.
     */
    @NonNull
    @Builder.Default
    Timings defaultTimings = Timings.lan();

    /**
     * Whether the served nodes log who voted for whom at INFO rather than DEBUG.
     *
     * <p>On by default, and worth more here than anywhere else: the witness is the vote that breaks
     * the tie, so "did the witness grant it, and to whom?" is the first question asked after a
     * two-node pair failed over, or failed to. Witness-wide rather than per application, like the
     * timing profile, because it describes how this process talks about its work rather than
     * anything about a tenant's cluster.
     */
    @Builder.Default
    boolean voteLogging = true;
}
