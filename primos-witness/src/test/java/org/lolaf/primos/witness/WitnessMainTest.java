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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Peer;

/** The command line, which is the whole of the operator's interface to the process. */
class WitnessMainTest {

    @Test
    void the_defaults_are_the_documented_ones() {
        WitnessMain.Arguments arguments = WitnessMain.Arguments.parse(new String[0]);

        assertThat(arguments.configDirectory()).isEqualTo(Paths.get("/etc/primos-witness"));
        assertThat(arguments.nodeId())
                .as("the top of the id range, so it does not collide with application nodes"
                        + " numbered from zero upward")
                .isEqualTo(Peer.MAX_ID);
        assertThat(arguments.bindAddress()).isNull();
    }

    @Test
    void every_option_is_read() {
        WitnessMain.Arguments arguments = WitnessMain.Arguments.parse(
                new String[] {"--config", "/srv/witness", "--node-id", "900",
                        "--bind", "10.0.0.4:7420"});

        assertThat(arguments.configDirectory()).isEqualTo(Paths.get("/srv/witness"));
        assertThat(arguments.nodeId()).isEqualTo(900);
        assertThat(arguments.bindAddress().getPort()).isEqualTo(7420);
        assertThat(arguments.bindAddress().getHostString()).isEqualTo("10.0.0.4");
    }

    @Test
    void help_asks_for_nothing_else() {
        assertThat(WitnessMain.Arguments.parse(new String[] {"--help"}).isHelp()).isTrue();
        assertThat(WitnessMain.Arguments.parse(new String[] {"-h"}).isHelp()).isTrue();
    }

    @Test
    void a_mistake_names_itself_rather_than_being_ignored() {
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--nodeid", "3"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown option '--nodeid'");
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--config"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a value");
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--node-id", "x"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a number");
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--node-id", "9999"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--bind", "7420"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
        assertThatThrownBy(() -> WitnessMain.Arguments.parse(new String[] {"--bind", "host:0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1-65535");
    }

    @Test
    void the_usage_text_says_why_a_restart_is_the_risky_operation() {
        assertThat(WitnessMain.usage())
                .as("it is the first thing an operator reads, and the thing they most need to know"
                        + " before reaching for a restart")
                .contains("--config", "--node-id", "--bind")
                .contains("tolerates zero failures");
    }
}
