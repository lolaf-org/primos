# Primos

[![build](https://github.com/lolaf-org/primos/actions/workflows/build.yml/badge.svg)](https://github.com/lolaf-org/primos/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.txt)

**Raft leader election for clustered Java applications.** Every instance knows whether it is the leader or a follower,
and is told the moment that changes.

*Primus inter pares*: first among equals. Also *cousins* in Spanish and Portuguese: peers, one of whom is currently
first.

Primos is an ordinary library for Java 11 and up. It runs the leader-election half of Raft inside your process, over
UDP, and depends on nothing but `slf4j-api`.

## What you get

- **Raft elections with PreVote and CheckQuorum**: a rejoining node cannot force a failover nobody asked for, and an
  isolated leader stands down instead of reporting `LEADER` forever.
- **A witness** for the two-node active/passive pair: a voter that never leads, deployed in a third failure domain, so
  the pair survives losing one node. One witness serves many clusters, as a tarball or a container image.
- **Fast planned failover**: a departing leader hands over in about one round trip instead of an election timeout.
- **Authenticated datagrams**: an optional HMAC on every message, verified before anything is parsed.
- **Observability without dependencies**: a push-only stats listener that never allocates on the election loop, and
  an optional Micrometer binding.
- **Spring Boot**: a starter that configures a node from `application.properties` and can start and stop your beans
  as leadership comes and goes.

## What it is not

**Leader election only.** No replicated log, no state machine, no snapshots, no runtime membership changes. Your
application keeps its own state; primos only tells it who is currently in charge.

## Getting started

```xml
<dependency>
    <groupId>org.lolaf.primos</groupId>
    <artifactId>primos</artifactId>
    <version>${primos.version}</version>
</dependency>
```

```java
List<Peer> peers = Arrays.asList(
        Peer.application(2, "node-b", 7420),
        Peer.application(3, "node-c", 7420));

PrimosNode node = PrimosNode.builder()
        .clusterConfig(ClusterConfig.builder()
                .applicationName("orders")
                .nodeId(1)
                .peers(peers)
                .build())
        .clusterSecret(secret)
        .store(() -> new FileStateStore(Paths.get("/var/lib/orders/primos")))
        .transport(UdpTransport.builder()
                .bindAddress(new InetSocketAddress(7420))
                .peers(peers))
        .listener(new PrimosListener() {
            @Override
            public void onRoleChanged(Role previous, Role current, long term) {
                log.info("now {} in term {}", current, term);
            }
        })
        .build();

node.start();
```

Before acting on leadership, read [Using leadership safely](docs/using-leadership-safely.md): `role() == LEADER` is not
a sufficient guard for a side effect, and the page says what is.

## Documentation

Everything else is in the [documentation](docs/README.md).

## Status

Built and tested, not yet published to Maven Central. The API may still change before the first release.

## Licence

Apache License 2.0.
