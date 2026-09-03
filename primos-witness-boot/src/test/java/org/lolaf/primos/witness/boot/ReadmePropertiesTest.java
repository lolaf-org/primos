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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Every {@code primos.witness.*} key the README names is a key that exists.
 *
 * <p>The library holds its README's Java examples to a compiler; this module's examples are
 * properties, so this is the same rule with the tool that fits them. A renamed or deleted setting
 * breaks the build rather than quietly leaving the documentation wrong — which for an
 * operator-facing artifact is the kind of wrong that is only discovered at three in the morning.
 *
 * <p>Both sides are generated: the keys come out of the README, and the truth comes from the
 * {@code spring-configuration-metadata.json} the configuration processor writes from
 * {@link WitnessProperties}. Nothing here is a list somebody has to remember to update.
 */
class ReadmePropertiesTest {

    /**
     * Keys as they appear in prose, tables and properties blocks: backticked, in a table cell, or
     * on the left of an {@code =}. Indexed and mapped keys are normalised, since
     * {@code applications[0].name} and {@code keys.<name>.value} are one setting each.
     */
    private static final Pattern KEY = Pattern.compile("primos\\.witness\\.[A-Za-z0-9.\\[\\]<>*_-]+");

    @Test
    void every_property_the_readme_names_exists() throws IOException {
        Set<String> documented = keysIn(read(Paths.get("README.md")));
        Set<String> real = keysIn(read(Paths.get("target/classes/META-INF/spring-configuration-metadata.json")));

        assertThat(real)
                .as("the configuration processor produced no metadata at all, so this test would"
                        + " pass by knowing nothing")
                .isNotEmpty();
        assertThat(documented)
                .as("the README names settings this application does not have")
                .isSubsetOf(real);
    }

    /**
     * The keys in a file, normalised the way the metadata spells them: an index or a map key
     * becomes the property it belongs to, and a trailing dot from a sentence is not part of a name.
     */
    private static Set<String> keysIn(String text) {
        Set<String> keys = new LinkedHashSet<>();
        Matcher matcher = KEY.matcher(text);
        while (matcher.find()) {
            String key = matcher.group()
                    .replaceAll("\\[[^\\]]*\\]", "")
                    .replaceAll("<[^>]*>", "*");
            while (key.endsWith(".")) {
                key = key.substring(0, key.length() - 1);
            }
            // applications.name -> applications, keys.*.value -> keys: the metadata names the
            // collection, not what is inside it.
            key = key.replaceAll("^(primos\\.witness\\.(?:applications|keys))\\..*$", "$1");
            keys.add(key);
        }
        return keys;
    }

    private static String read(Path file) throws IOException {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
