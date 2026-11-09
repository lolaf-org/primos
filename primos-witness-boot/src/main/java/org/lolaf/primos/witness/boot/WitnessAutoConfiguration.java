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

import org.lolaf.primos.micrometer.WitnessMetrics;
import org.lolaf.primos.time.MonotonicClock;
import org.lolaf.primos.witness.ConfigStore;
import org.lolaf.primos.witness.FileConfigStore;
import org.lolaf.primos.witness.WitnessServer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
     * The served set and the keys.
     *
     * <p>A file store by default, and the reason is the one thing this packaging could quietly
     * lose: onboarding a cluster must never need a restart. The properties store is opt-in and says
     * what it costs when it is chosen. See {@link WitnessProperties}.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ConfigStore witnessConfigStore(WitnessProperties properties) {
        if (properties.configStore() == WitnessProperties.Store.PROPERTIES) {
            return new PropertiesConfigStore(properties);
        }
        refuseSettingsThatTheFileOwns(properties);
        if (!Files.isDirectory(properties.configDirectory())) {
            // Checked here rather than left to the first read, so the message names the property
            // an operator has to fix instead of arriving as a file-not-found several layers down.
            throw new IllegalStateException("no configuration directory at "
                    + properties.configDirectory() + "; set primos.witness.config-directory."
                    + " It must hold witness.properties and an applications/ directory.");
        }
        return new FileConfigStore(properties.configDirectory(), MonotonicClock.SYSTEM);
    }

    private void refuseSettingsThatTheFileOwns(WitnessProperties properties) {
        List<String> misplaced = new ArrayList<>();
        if (properties.stateDirectory() != null) {
            misplaced.add("state-directory");
        }
        if (!properties.applications().isEmpty()) {
            misplaced.add("applications");
        }
        if (!properties.keys().isEmpty()) {
            misplaced.add("keys");
        }
        if (misplaced.isEmpty()) {
            return;
        }
        throw new IllegalStateException("primos.witness." + String.join(", primos.witness.",
                misplaced) + " " + (misplaced.size() == 1 ? "is" : "are") + " only read with"
                + " primos.witness.config-store=properties. Under the default file store these come"
                + " from witness.properties in the configuration directory, which is re-read while"
                + " the server runs, so binding them here would freeze them at boot and change"
                + " nothing an operator could see. Move them into witness.properties, or switch the"
                + " store and accept that onboarding a cluster then needs a restart.");
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
    public WitnessServer witnessServer(ConfigStore configStore, WitnessProperties properties,
                                       ObjectProvider<WitnessMetrics> metrics) {
        WitnessServer.Builder server = WitnessServer.builder()
                .configStore(configStore)
                .nodeId(properties.nodeId());
        if (properties.bind().isSet()) {
            server.bindAddress(bindAddress(properties.bind(), configStore.current().settings().bindAddress()));
        }
        // A tenant's sink has to be in place before build(), because the constructor opens every
        // application the configuration directory already names.
        WitnessMetrics bound = metrics.getIfAvailable();
        if (bound != null) {
            server.statsListeners(bound.statsListeners());
        }
        WitnessServer built = server.build();
        if (bound != null) {
            bound.bind(built);
        }
        return built;
    }

    /**
     * What runs the loop, and what stops it when the context closes.
     *
     * <p>The bound on the stop is Spring's own budget for this shutdown phase rather than a number
     * invented here, so an operator who lengthens one has lengthened both. Read from the
     * {@link Environment} rather than through a {@code @Value} placeholder, because placeholders
     * need a resolver that an auto-configuration cannot assume is there, and the first thing that
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

    private static InetSocketAddress bindAddress(WitnessProperties.BindProperties bind, InetSocketAddress configured) {
        // Either half may be given alone; the other comes from the file rather than from a default invented here.
        int port = bind.port() != null ? bind.port() : configured.getPort();
        if (bind.host() == null) {
            return new InetSocketAddress(configured.getAddress(), port);
        }
        return bind.host().isEmpty()
                ? new InetSocketAddress(port)
                : new InetSocketAddress(bind.host(), port);
    }

    /**
     * The health endpoint's view of this witness, only where actuator health is on the classpath. A nested class,
     * because Spring reads every method of a configuration class before any condition: a {@code @Bean} method
     * returning a {@code HealthIndicator} on the outer class would fail the whole auto-configuration without it.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
    static class Health {

        @Bean
        @ConditionalOnMissingBean
        public WitnessHealthIndicator witnessHealthIndicator(WitnessServer server, WitnessServerLifecycle lifecycle) {
            return new WitnessHealthIndicator(server, lifecycle);
        }
    }
}
