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
package org.lolaf.primos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The arithmetic of a cluster's voter set, and the warnings that arithmetic deserves.
 *
 * <p>Quorum is always a majority of the <em>configured</em> voters, never of the reachable ones. A
 * cluster of five with two down still needs three votes; anything else is a split brain waiting to
 * be discovered.
 *
 * <p>Every node logs {@link #summary()} at startup, because it is the first thing anyone will want
 * when asking why a cluster did not fail over, and logging it beats reconstructing the voter set
 * from config files. It also logs any {@link #warnings()}: a configuration can be strictly worse
 * than an available alternative without being wrong, and primos says so rather than refusing to
 * start — refusing would break a legitimate transient, such as keeping a witness while scaling a
 * pair up to three nodes.
 */
@Getter
@EqualsAndHashCode
@ToString
public final class Quorum {

    /**
     * Above this many voters, primos says so.
     *
     * <p>Raft clusters do not get more available by getting bigger: every added voter is another
     * thing that can fail, quorum grows with it, and elections take longer to conclude. Three and
     * five are the usual answers, seven is already unusual. primos also probes every peer from
     * every peer, so reachability traffic grows with the square of the cluster.
     *
     * <p>It is a warning rather than a refusal, like every other quorum-shape judgement here: a
     * large cluster is suboptimal, not incorrect.
     */
    public static final int MAX_RECOMMENDED_VOTERS = 7;

    private final String applicationName;
    private final int applicationNodes;
    private final boolean witness;
    private final int voters;
    private final int required;
    private final int faultTolerance;

    private Quorum(String applicationName, int applicationNodes, boolean witness) {
        this.applicationName = Objects.requireNonNull(applicationName, "applicationName");
        if (applicationNodes < 1) {
            throw new IllegalArgumentException(
                    "a cluster has at least one application node, but was " + applicationNodes);
        }
        this.applicationNodes = applicationNodes;
        this.witness = witness;
        this.voters = applicationNodes + (witness ? 1 : 0);
        this.required = voters / 2 + 1;
        this.faultTolerance = voters - required;
    }

    /** The arithmetic implied by a configuration. */
    public static Quorum of(ClusterConfig config) {
        Objects.requireNonNull(config, "config");
        return new Quorum(config.applicationName(), config.applicationNodeCount(),
                config.hasWitness());
    }

    /** Whether a given number of votes is a majority of the configured voters. */
    public boolean isReached(int votes) {
        return votes >= required;
    }

    /** The line every node logs at startup. */
    public String summary() {
        return "cluster '" + applicationName + "': " + voters + " voters ("
                + composition() + "), quorum " + required + ", tolerates " + failures(faultTolerance);
    }

    /**
     * Configurations that are strictly worse than an available alternative, each naming the
     * alternative and its numbers. Empty when there is nothing useful to say.
     */
    public List<String> warnings() {
        List<String> warnings = new ArrayList<>(2);

        if (voters > MAX_RECOMMENDED_VOTERS) {
            warnings.add("cluster '" + applicationName + "': " + voters + " voters is larger than"
                    + " Raft clusters benefit from. More voters means more things that can fail and"
                    + " a larger quorum, not better availability, and probe traffic grows with the"
                    + " square of the cluster size. Three or five is the usual answer.");
        }

        if (witness && applicationNodes % 2 == 1) {
            int withoutWitness = applicationNodes / 2 + 1;
            warnings.add("cluster '" + applicationName + "': the witness does not help here."
                    + " With it, " + voters + " voters need " + required + " for quorum; the "
                    + applicationNodes + " application nodes alone would need " + withoutWitness
                    + " of " + applicationNodes + " and tolerate the same "
                    + failures(faultTolerance) + ". Consider removing it.");
        } else if (!witness && applicationNodes == 2) {
            warnings.add("cluster '" + applicationName + "': 2 nodes with no witness tolerates"
                    + " zero failures. Any single failure or partition leaves this cluster with no"
                    + " leader at all, which is worse than running one node. Add a witness in a"
                    + " third failure domain to make it 3 voters with quorum 2.");
        } else if (!witness && applicationNodes % 2 == 0) {
            int withWitness = (applicationNodes + 1) / 2 + 1;
            warnings.add("cluster '" + applicationName + "': " + voters + " voters tolerate "
                    + failures(faultTolerance) + ". Adding a witness would make " + (voters + 1)
                    + " voters needing " + withWitness + ", tolerating "
                    + failures(voters + 1 - withWitness) + ".");
        }
        return Collections.unmodifiableList(warnings);
    }

    private String composition() {
        String nodes = applicationNodes + (applicationNodes == 1 ? " node" : " nodes");
        return witness ? nodes + " + witness" : nodes;
    }

    private static String failures(int count) {
        return count + (count == 1 ? " failure" : " failures");
    }
}
