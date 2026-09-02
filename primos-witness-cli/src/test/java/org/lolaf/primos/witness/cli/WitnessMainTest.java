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
package org.lolaf.primos.witness.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.lolaf.primos.Peer;

import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

/** The command line, which is the whole of the operator's interface to the process. */
class WitnessMainTest {

    @Test
    void the_defaults_are_the_documented_ones() {
        WitnessMain command = parse();

        assertThat(command.configDirectory()).isEqualTo(Paths.get("/etc/primos-witness"));
        assertThat(command.nodeId())
                .as("the top of the id range, so it does not collide with application nodes"
                        + " numbered from zero upward")
                .isEqualTo(Peer.MAX_ID);
        assertThat(command.bindAddress()).isNull();
    }

    @Test
    void every_option_is_read() {
        WitnessMain command = parse("--config", "/srv/witness", "--node-id", "900",
                "--bind", "10.0.0.4:7420");

        assertThat(command.configDirectory()).isEqualTo(Paths.get("/srv/witness"));
        assertThat(command.nodeId()).isEqualTo(900);
        assertThat(command.bindAddress().getPort()).isEqualTo(7420);
        assertThat(command.bindAddress().getHostString()).isEqualTo("10.0.0.4");
    }

    @Test
    void a_bind_with_no_host_is_the_wildcard_address() {
        assertThat(parse("--bind", ":7420").bindAddress().getPort()).isEqualTo(7420);
    }

    @Test
    void help_asks_for_nothing_else() {
        assertThat(new CommandLine(new WitnessMain()).parseArgs("--help").isUsageHelpRequested())
                .isTrue();
        assertThat(new CommandLine(new WitnessMain()).parseArgs("-h").isUsageHelpRequested())
                .isTrue();
    }

    @Test
    void the_version_is_answerable_without_unpacking_the_jar() {
        assertThat(new CommandLine(new WitnessMain()).parseArgs("--version")
                .isVersionHelpRequested()).isTrue();
        assertThat(new WitnessMain.BuildVersion().getVersion())
                .as("read from the manifest when there is one, and never absent, since an"
                        + " operator asking this is trying to establish what is deployed")
                .hasSize(1)
                .allSatisfy(line -> assertThat(line).startsWith("primos-witness "));
    }

    @Test
    void a_mistake_names_itself_rather_than_being_ignored() {
        assertThatThrownBy(() -> parse("--nodeid", "3"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("--nodeid");
        assertThatThrownBy(() -> parse("--config"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("--config");
        assertThatThrownBy(() -> parse("--node-id", "x"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("must be a number");
        assertThatThrownBy(() -> parse("--node-id", "9999"))
                .as("the id range is checked where it is typed, not several layers down")
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining(String.valueOf(Peer.MAX_ID));
        assertThatThrownBy(() -> parse("--bind", "7420"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("host:port");
        assertThatThrownBy(() -> parse("--bind", "host:0"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("1-65535");
    }

    @Test
    void a_missing_configuration_directory_is_the_operator_s_mistake_not_a_crash() {
        WitnessMain command = new WitnessMain();
        int status = new CommandLine(command).execute("--config", "/no/such/directory/anywhere");

        assertThat(status)
                .as("exit 2 with the usage text, the way a mistyped option is reported — an init"
                        + " system reading this must not be told the software failed")
                .isEqualTo(CommandLine.ExitCode.USAGE);
    }

    @Test
    void the_usage_text_says_why_a_restart_is_the_risky_operation() {
        assertThat(WitnessMain.usage())
                .as("it is the first thing an operator reads, and the thing they most need to know"
                        + " before reaching for a restart")
                .contains("--config", "--node-id", "--bind")
                .contains("tolerates zero failures");
    }

    @Test
    void the_options_are_listed_in_the_order_they_matter() {
        String usage = WitnessMain.usage();

        assertThat(usage.indexOf("--config"))
                .as("alphabetical order would put --bind first, which is the least important of"
                        + " the three and the one most operators never pass")
                .isLessThan(usage.indexOf("--node-id"));
        assertThat(usage.indexOf("--node-id")).isLessThan(usage.indexOf("--bind"));
    }

    private static WitnessMain parse(String... args) {
        WitnessMain command = new WitnessMain();
        new CommandLine(command).parseArgs(args);
        return command;
    }
}
