# Primos documentation

Start with [Building a node](building-a-node.md), then read [Using leadership safely](using-leadership-safely.md)
before your application acts on being leader.

## Guides

| guide | what it answers |
|-------|-----------------|
| [Building a node](building-a-node.md) | How do I describe a cluster, start a node in it, and hear when leadership changes? |
| [Using leadership safely](using-leadership-safely.md) | Why is `role() == LEADER` not enough to guard a side effect, and what is? |
| [Cluster sizing and the witness](cluster-sizing.md) | How many voters do I need, why does a pair need a witness, and what does a one-node cluster do? |
| [Clean shutdown](clean-shutdown.md) | Why does a planned restart cost one round trip instead of an election timeout? |
| [Planned switchover and maintenance](leadership-control.md) | How do I move leadership without stopping a node, or keep a node from leading? |
| [Timings](timings.md) | Which profile do I pick, what does failover cost, and how do I derive my own intervals? |
| [Leader data](leader-data.md) | How does a leader tell every node that its data changed, so each can fetch it, for sharding for example? |
| [Observability](observability.md) | What does a node report as it runs, and why is there no stats snapshot? |
| [Micrometer](micrometer.md) | Which meters do I get on my `MeterRegistry`, and which one do I alert on? |
| [Spring Boot](spring-boot.md) | How do I configure a node from `application.properties` and gate my beans on leadership? |
| [Design](design.md) | Why UDP, why one thread, what is persisted when, and how is it tested? |
| [Wire protocol](protocol.md) | How closely does primos follow Raft, and what is in every message on the wire? |

## Running a witness

| guide | what it answers |
|-------|-----------------|
| [Witness manual](witness.md) | What are the safety rules, how do I configure and rotate keys, and what do I alert on? |
| [Witness from the tarball](witness-cli.md) | How do I install the witness as a plain process under systemd? |
| [Witness on Spring Boot and containers](witness-spring-boot.md) | How do I run the witness as a Boot application, a container image or a Kubernetes StatefulSet? |
