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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The whole configuration at one instant, immutable.
 *
 * <p>The server reads a volatile reference to one of these; a reload publishes a new one in a
 * single write. There is no lock, no torn configuration, and no way to serve half an update.
 */
public final class ConfigSnapshot {

    private final WitnessSettings settings;
    private final Map<String, ApplicationConfig> applications;

    public ConfigSnapshot(WitnessSettings settings, Map<String, ApplicationConfig> applications) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.applications = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(applications, "applications")));
    }

    public WitnessSettings settings() {
        return settings;
    }

    public Map<String, ApplicationConfig> applications() {
        return applications;
    }

    public Set<String> applicationNames() {
        return applications.keySet();
    }

    public ApplicationConfig application(String name) {
        return applications.get(name);
    }

    public boolean serves(String name) {
        return applications.containsKey(name);
    }

    /** The same settings with one more application, for permissive auto-registration. */
    public ConfigSnapshot with(ApplicationConfig application) {
        Map<String, ApplicationConfig> extended = new LinkedHashMap<>(applications);
        extended.put(application.name(), application);
        return new ConfigSnapshot(settings, extended);
    }

    @Override
    public String toString() {
        return "ConfigSnapshot(" + applications.size() + " applications: "
                + String.join(", ", applications.keySet()) + ")";
    }
}
