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
package org.lolaf.primos.witness.boot;

import org.lolaf.primos.Peer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.nio.file.Path;

/**
 * The three settings Spring binds, and deliberately no more.
 *
 * <p>These are the tarball's three command-line options, under the names
 * {@code witness.properties} already uses, so an operator moving between packagings is not learning
 * a second vocabulary. <strong>Everything else — the state directory, {@code config.source}, the
 * keyring, the timings, vote logging, the rescan interval — keeps coming from
 * {@code witness.properties} through {@link org.lolaf.primos.witness.FileConfigStore}, and that is
 * a rule rather than an omission.</strong>
 *
 * <p>Spring binds once, at boot. A key bound in both places would be silently frozen at the value
 * Spring read while an operator edited the file and watched nothing happen — and the file is
 * re-read on purpose, because <em>restarting a witness is the one genuinely risky operation in its
 * lifecycle</em>: while it is down, every two-node cluster it serves tolerates zero failures. The
 * keyring settles it on its own: a secret bound at boot cannot be rotated without the restart this
 * whole design exists to avoid.
 *
 * <p>A record with constructor binding rather than a bean with setters, for the reason
 * {@code PrimosProperties} gives in the other Spring module: this project configures Lombok with
 * fluent accessors, and Spring's JavaBean binder would not find {@code nodeId()}.
 *
 * @param configDirectory where {@code witness.properties} and {@code applications/} live. The
 *                        tarball's {@code --config}, and the same default.
 * @param nodeId          this witness's node id. The tarball's {@code --node-id}. Durable in every
 *                        served application's recorded vote, so treat it as fixed once deployed.
 * @param bind            overrides {@code bind.host} and {@code bind.port} from the file, the way
 *                        the tarball's {@code --bind} does
 */
@ConfigurationProperties("primos.witness")
public record WitnessProperties(
        @DefaultValue(WitnessProperties.DEFAULT_CONFIG_DIRECTORY) Path configDirectory,
        @DefaultValue("" + Peer.MAX_ID) int nodeId,
        @DefaultValue BindProperties bind) {

    /**
     * The tarball's default, repeated here so both packagings look in the same place when nobody
     * says otherwise.
     */
    static final String DEFAULT_CONFIG_DIRECTORY = "/etc/primos-witness";

    /**
     * Where to listen, when the environment rather than the file is saying.
     *
     * <p>Split in two, as {@code bind.host} and {@code bind.port} are in the file, so that a
     * container can set the port alone — which is the common case — without having to restate the
     * host. Either may be given by itself; whichever is missing comes from the file.
     *
     * @param host the local address, or the wildcard when neither this nor the file names one
     * @param port the local port
     */
    public record BindProperties(String host, Integer port) {

        boolean isSet() {
            return host != null || port != null;
        }
    }
}
