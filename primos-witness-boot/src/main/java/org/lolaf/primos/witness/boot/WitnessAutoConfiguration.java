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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.net.InetSocketAddress;
import java.nio.file.Files;

@AutoConfiguration
@ConditionalOnClass(WitnessServer.class)
@EnableConfigurationProperties(WitnessProperties.class)
public class WitnessAutoConfiguration {

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