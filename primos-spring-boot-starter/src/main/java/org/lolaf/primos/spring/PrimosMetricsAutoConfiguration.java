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
    package org.lolaf.primos.spring;

    import io.micrometer.core.instrument.MeterRegistry;
    import org.lolaf.primos.ClusterConfig;
    import org.lolaf.primos.PrimosNode;
    import org.lolaf.primos.micrometer.PrimosMetrics;
    import org.springframework.beans.factory.SmartInitializingSingleton;
    import org.springframework.boot.autoconfigure.AutoConfiguration;
    import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
    import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
    import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
    import org.springframework.context.annotation.Bean;

    /**
     * Binds the node onto the application's {@link MeterRegistry}, when it has one and
     * {@code primos-micrometer} is on the classpath.
     *
     * <p>The two halves land in different places because they must: {@link PrimosMetrics} is a
     * {@code PrimosStatsListener}, so {@link PrimosAutoConfiguration} picks it up and installs it while
     * the node is being built — a sink fixed at build time cannot be added afterwards — and the gauges
     * are bound once every singleton exists, which is still before the node is started.
     */
    @AutoConfiguration(after = PrimosAutoConfiguration.class,
            afterName = "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration")
    @ConditionalOnClass({MeterRegistry.class, PrimosMetrics.class})
    @ConditionalOnBean(MeterRegistry.class)
    public class PrimosMetricsAutoConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public PrimosMetrics primosMetrics(MeterRegistry registry, ClusterConfig cluster) {
            return PrimosMetrics.create(registry, cluster);
        }

        /**
         * Gauges poll the node, so they cannot be registered until it exists.
         */
        @Bean
        public SmartInitializingSingleton primosMetricsBinding(PrimosMetrics metrics, PrimosNode node) {
            return () -> metrics.bind(node);
        }
    }
