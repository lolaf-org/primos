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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/** The checks that only exist because an application name arrives from the network. */
class ApplicationNamesTest {

    private static final List<String> SERVED = Arrays.asList("orders", "billing", "shipping", "db");

    @Test
    void the_transposition_that_the_whole_mechanism_exists_for_is_caught() {
        assertThat(ApplicationNames.nearMiss("ordres", SERVED))
                .as("two nodes named 'orders' and 'ordres' cannot see each other, each asks the"
                        + " witness, and a permissive witness grants a vote from each bucket — two"
                        + " leaders out of one typo, with nothing else in the system able to see it")
                .isEqualTo("orders");
    }

    @Test
    void a_missing_letter_a_doubled_letter_and_a_wrong_case_are_all_caught() {
        assertThat(ApplicationNames.nearMiss("order", SERVED)).isEqualTo("orders");
        assertThat(ApplicationNames.nearMiss("orderss", SERVED)).isEqualTo("orders");
        assertThat(ApplicationNames.nearMiss("Orders", SERVED)).isEqualTo("orders");
        assertThat(ApplicationNames.nearMiss("billling", SERVED)).isEqualTo("billing");
    }

    @Test
    void a_genuinely_different_name_is_not_flagged() {
        assertThat(ApplicationNames.nearMiss("payments", SERVED)).isNull();
        assertThat(ApplicationNames.nearMiss("orders", SERVED))
                .as("an exact match is the served application itself, not a near miss")
                .isNull();
    }

    @Test
    void short_names_are_not_compared_at_all() {
        assertThat(ApplicationNames.nearMiss("dc", SERVED))
                .as("at three characters and under, half the names in any estate are one edit"
                        + " apart, so the warning would be noise rather than signal")
                .isNull();
        assertThat(ApplicationNames.nearMiss("dbx", SERVED)).isNull();
    }

    @Test
    void nothing_is_flagged_when_nothing_is_served_yet() {
        assertThat(ApplicationNames.nearMiss("orders", Collections.emptyList())).isNull();
    }

    @Test
    void a_name_that_would_choose_its_own_state_directory_is_not_usable() {
        assertThat(ApplicationNames.isUsableAsDirectory("orders")).isTrue();
        assertThat(ApplicationNames.isUsableAsDirectory("orders-eu.1")).isTrue();

        assertThat(ApplicationNames.isUsableAsDirectory("../../etc")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory("a/b")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory("a\\b")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory("c:name")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory("..")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory(".")).isFalse();
        assertThat(ApplicationNames.isUsableAsDirectory("")).isFalse();
    }
}
