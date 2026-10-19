# Design

## In one page

- **One dependency.** `slf4j-api` at compile scope and nothing else; Lombok is compile-time only and not transitive.
  Plain JDK concurrency and NIO throughout, which is why the wire codec and the configuration parser are hand-written.
- **UDP transport.** Raft already assumes a network that drops, delays, duplicates and reorders, and recovers from all
  of it with its own timers, so TCP's retransmission is duplicated work. Nine message types, all fixed-size, the
  largest 104 bytes including its authentication tag, so every one fits an unfragmented datagram. See
  [Wire protocol](protocol.md).
- **Optional HMAC on every datagram**, verified before anything is parsed. There is deliberately no "authenticated"
  flag in the header and nothing on the wire naming the algorithm, because an attacker would clear one and downgrade
  the other.
- **One thread per node.** A single NIO `Selector` loop is the only blocking point for both datagrams and timers.
- **Persist before acting.** `currentTerm` and `votedFor` are fsynced before any message that depends on them is sent.
  A node that grants a vote and then forgets it can grant a second in the same term, and two candidates each collect a
  majority.
- **Votes counted in a set, never a counter.** UDP duplicates datagrams; a counter would let one voter elect a leader.
- **CheckQuorum and PreVote are mandatory.** An isolated leader stands down instead of reporting `LEADER` forever; a
  rejoining node cannot force a failover nobody asked for. Both matter more here than in full Raft, because with no log
  the only thing a leader produces is the answer to *am I the leader?*
- **Quorum from configured membership.** Never from who is reachable, and never shrunk by forgetting a peer.

Where the code departs from figure 2 of the Raft paper, a comment at the site says so.

## Modules

| module | what |
|---|---|
| `primos` | the library: API, role state machine, wire codec, transports, state store |
| `primos-witness` | the witness itself and its configuration store, as a library with no `main` |
| `primos-micrometer` | meters for a node and for a witness's served clusters; see [Micrometer](micrometer.md) |
| `primos-witness-cli` | the command line and the `tar.gz`/`zip` distribution; see [Witness from the tarball](witness-cli.md) |
| `primos-witness-boot` | the witness as a Spring Boot application, and the container image; see [Witness on Spring Boot](witness-spring-boot.md) |
| `primos-spring-boot-starter` | Spring Boot auto-configuration and a leadership-gated lifecycle; see [Spring Boot](spring-boot.md) |

The library and the witness are two artifacts rather than the usual api/impl split, deliberately: module splits exist
to isolate dependencies, and primos has none to isolate. The Spring Boot modules are the exception that proves the
rule: Spring Boot is exactly the transitive tree a consumer must be able to decline, so it lives in its own artifacts
and the library references no Spring type. They are also the only modules not on Java 11, because Spring Boot's floor
is 17.

`org.lolaf.primos.core` and `org.lolaf.primos.wire` are internal. Their classes are public only because the witness is
a separate artifact; each package's `package-info` says so.

## Testing

**Deterministic simulation is the primary harness**: N nodes on one thread over a virtual clock and an in-memory
transport that drops, delays, reorders and partitions on a seeded PRNG. Every scenario runs under continuously checked
safety invariants (at most one leader per term, a node never contradicting a vote it gave before a crash, a witness
never becoming candidate or leader), and **each invariant has a test proving it fires**, so a green suite means
something.

Real sockets and real threads are a sanity check on top of that, not the safety net.

The code examples in this documentation are compiled and run as tests, so an API change that makes them wrong breaks
the build.

## Building from source

```
./mvnw verify
```

JDK 17 or newer builds every module; CI runs on 17 and 25. The library and the witness are compiled with
`maven.compiler.release=11`, so they run on Java 11.
