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

import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.ConfigStore;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;

/**
 * Builds a {@link WitnessServer} from {@code primos.witness.*} and the configuration directory,
 * and runs it with the context.
 *
 * <p><strong>This module is a front door and holds no logic of its own.</strong> The routing, the
 * per-application state, the admission checks and every invariant are in {@code primos-witness} and
 * are shared, unchanged, with the tarball. If anything here grows behaviour, or if anything is
 * copied between the two mains, that is the thing to delete.
 *
 * <p>Every bean backs off if the application defines its own, which is how the opt-in
 * properties-only {@link ConfigStore} arrives without a switch in this class.
 */
@AutoConfiguration
@ConditionalOnClass(WitnessServer.class)
@EnableConfigurationProperties(WitnessProperties.class)
public class WitnessAutoConfiguration {

    /**
     * The served set and the keys, re-read while the server runs.
     *
     * <p>A file store rather than the properties Spring has already bound, and the reason is the
     * one thing this packaging could quietly lose: onboarding a cluster must never need a restart.
     * See {@link WitnessProperties}.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ConfigStore witnessConfigStore(WitnessProperties properties) {
        if (!Files.isDirectory(properties.configDirectory())) {
            // Checked here rather than left to the first read, so the message names the property
            // an operator has to fix instead of arriving as a file-not-found several layers down.
            throw new IllegalStateException("no configuration directory at "
                    + properties.configDirectory() + "; set primos.witness.config-directory."
                    + " It must hold witness.properties and an applications/ directory.");
        }
        return new FileConfigStore(properties.configDirectory(), MonotonicClock.SYSTEM);
    }

    /**
     * The server itself, built here and run by {@link WitnessServerLifecycle}.
     *
     * <p>Closed by Spring as well as stopped by it, because the socket and every application's
     * state store are taken when the server is <em>built</em> rather than when it starts. A context
     * that failed to refresh after this bean existed would otherwise hold both until the JVM
     * exited.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public WitnessServer witnessServer(ConfigStore configStore, WitnessProperties properties) {
        WitnessServer.Builder server = WitnessServer.builder()
                .configStore(configStore)
                .nodeId(properties.nodeId());
        InetSocketAddress bind = bindAddress(configStore, properties);
        if (bind != null) {
            server.bindAddress(bind);
        }
        return server.build();
    }

    /**
     * What runs the loop, and what stops it when the context closes.
     *
     * <p>The bound on the stop is Spring's own budget for this shutdown phase rather than a number
     * invented here, so an operator who lengthens one has lengthened both. Read from the
     * {@link Environment} rather than through a {@code @Value} placeholder, because placeholders
     * need a resolver that an auto-configuration cannot assume is there — and the first thing that
     * finds out is a test context, which is exactly where this bean must also work.
     */
    @Bean
    @ConditionalOnMissingBean
    public WitnessServerLifecycle witnessServerLifecycle(WitnessServer server,
                                                        Environment environment) {
        Duration stopTimeout = environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase",
                Duration.class, Duration.ofSeconds(30));
        return new WitnessServerLifecycle(server, stopTimeout);
    }

    /**
     * The health endpoint's view of this witness.
     *
     * <p>Contributed only when the application actually has actuator health on its classpath, so
     * the module still works in a deployment that has stripped it.
     */
    @Bean
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnMissingBean
    public WitnessHealthIndicator witnessHealthIndicator(WitnessServer server,
                                                         WitnessServerLifecycle lifecycle) {
        return new WitnessHealthIndicator(server, lifecycle);
    }

    /**
     * What {@code primos.witness.bind.*} overrides, or null to leave the file alone.
     *
     * <p>Either half may be given by itself — a container setting the port and nothing else is the
     * common case — so the half that was not given is taken from the address the file already
     * produced rather than from a default invented here. That keeps one answer to "what does this
     * witness bind to?" instead of two that can disagree.
     */
    private InetSocketAddress bindAddress(ConfigStore configStore, WitnessProperties properties) {
        WitnessProperties.BindProperties bind = properties.bind();
        if (!bind.isSet()) {
            return null;
        }
        InetSocketAddress configured = configStore.current().settings().bindAddress();
        int port = bind.port() != null ? bind.port() : configured.getPort();
        if (bind.host() == null) {
            return new InetSocketAddress(configured.getAddress(), port);
        }
        return bind.host().isEmpty()
                ? new InetSocketAddress(port)
                : new InetSocketAddress(bind.host(), port);
    }
}
