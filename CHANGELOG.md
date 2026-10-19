# Changelog

All notable changes to this project are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each released version needs its own `## [x.y.z] - YYYY-MM-DD` heading here **before** the release is
cut: the release workflow refuses to run without one, and the GitHub Release for the tag is created
with that section as its body. Write it in the commit that precedes the release, together with the
matching `[x.y.z]:` link definition at the foot of the file.

There is deliberately no `[Unreleased]` section, which is where Keep a Changelog would collect notes
between releases. A section is written when the version it belongs to is being cut, so its heading
carries the right number and date the first time and the workflow's check has exactly one heading it
could mean.

## [0.9.0] - WIP

First public release. Primos runs the leader-election half of Raft inside a Java application, so a set of instances
elect a leader among themselves and each is told the moment that changes. It is an ordinary library, over UDP, with
no dependency but SLF4J. It does leader election only: there is no replicated log, and the application keeps its own
state.

### Added

- **Leader election.** `PrimosNode` joins a cluster described by a `ClusterConfig` and reports its role, its term and
  the current leader, through calls and through `PrimosListener` callbacks. Pre-vote stops a node that was cut off from
  forcing an election when it comes back, and a leader that loses contact with a majority stands down instead of
  believing it still leads.
- **The term as a fencing token.** `roleAndTerm()` reads the role and the term together, so work done as leader can
  carry the term to an external system that rejects anything older. The documentation explains why a role check alone
  is not enough.
- **Leader leases.** `leadershipConfirmedNanos()` says, while a node leads, by when a quorum had last answered it. A
  lease that ends a fixed time after that cannot outlive the leadership, because a successor is only elected once a
  quorum has heard nothing from the leader for an election timeout.
- **Durable votes.** `FileStateStore` writes the term and the vote to disk before acting on them, so a node that
  restarts cannot vote twice in one term.
- **Fast planned failover.** A leader that stops hands leadership to a follower in about one round trip instead of an
  election timeout, and any node that stops tells its peers, so they report it down at once rather than after a probe
  timeout. `onPeerDown` says which it was: `LEFT` for a clean stop, `UNREACHABLE` for a peer that went silent and may
  come back.
- **Restartable nodes.** A stopped node can be started again and rejoins with the term and vote it left with.
- **Leader data.** A leader calls `publishChange()` when data it owns changes, a shard assignment for example, and
  every node is told through `onLeaderVersion`, so each can fetch the data from the leader over its own channel. The
  data never goes through primos, so it can be any size. With Spring Boot it arrives as a `PrimosEvents.LeaderVersion`
  event, and `primos-micrometer` counts versions published and received.
- **Authentication.** With a cluster secret set, every datagram carries an HMAC (SHA-256 by default) and is checked
  before anything in it is read.
- **Timing profiles.** `Timings.lan()` and `Timings.wan()`, or your own intervals, which are refused when they
  form a combination that would cause spurious elections. Nodes compare their intervals and log a mismatch.
- **The witness.** A voter that never leads, so a two-node pair survives losing one node. One witness serves many
  clusters, each with its own state, with keys that can be shared by a fleet or dedicated to one tenant, rotated
  without a restart, and configuration re-read while it runs.
  - `primos-witness-cli-0.9.0.tar.gz` on the GitHub Release, a plain process to run under systemd.
  - `ghcr.io/lolaf-org/primos-witness:0.9.0`, the same witness as a Spring Boot application with a health endpoint and
    metrics.
  - `primos-witness` on Maven Central, to embed the witness in your own process.
- **Observability.** `PrimosStatsListener` reports elections and how long they took, how long a cluster had no
  leader, handovers, probe round trips and dropped datagrams as they happen, without allocating on the election
  thread. `primos-micrometer` binds a node or a witness onto a Micrometer `MeterRegistry`.
- **Spring Boot.** `primos-spring-boot-starter` configures a node from `application.properties`, publishes leadership
  changes as Spring events, reports health, and can start and stop your beans as this node gains and loses
  leadership.

### Notes

- Java 11 or later for `primos`, `primos-witness` and `primos-micrometer`. Java 17 and Spring Boot 4 for the starter
  and the Spring Boot witness.
- The wire protocol, version 1, is described in `docs/protocol.md`.

[0.9.0]: https://github.com/lolaf-org/primos/releases/tag/v0.9.0
