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

import io.micrometer.core.instrument.MeterRegistry;
import org.lolaf.primos.micrometer.WitnessMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * The witness's routing counters, and a full set of node meters per served cluster.
 *
 * <p>Ordered before {@link WitnessAutoConfiguration} rather than after it, because the per-tenant
 * sinks must be handed to the builder: the server's constructor opens every application the
 * configuration directory already names, so a listener installed afterwards would miss them.
 */
@AutoConfiguration(before = WitnessAutoConfiguration.class,
        afterName = "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass({MeterRegistry.class, WitnessMetrics.class})
@ConditionalOnBean(MeterRegistry.class)
@EnableConfigurationProperties(WitnessProperties.class)
public class WitnessMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public WitnessMetrics witnessMetrics(MeterRegistry registry, WitnessProperties properties) {
        return WitnessMetrics.create(registry, properties.nodeId());
    }
}
