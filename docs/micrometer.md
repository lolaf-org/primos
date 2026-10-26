# Micrometer

`primos-micrometer` binds a primos node, a witness server and every cluster it serves onto whatever
[Micrometer](https://micrometer.io) `MeterRegistry` your application already runs.

It is a separate artifact so that `primos` itself never references a Micrometer type: an application that wants no
Micrometer receives none. With the [Spring Boot starter](spring-boot.md#metrics) it binds itself, with nothing to
configure.

```xml
<dependency>
    <groupId>org.lolaf.primos</groupId>
    <artifactId>primos-micrometer</artifactId>
    <version>0.9.0</version>
</dependency>
```

## A node

Two calls, and they cannot be one. The counters and timers arrive through `PrimosStatsListener`, which is a single
sink fixed when the node is built, so the metrics object has to exist first; the gauges poll the node, so they cannot
be registered until it exists.

```java
ClusterConfig cluster = ClusterConfig.builder()
        .applicationName("orders")
        .nodeId(1)
        .peer(Peer.application(2, "127.0.0.1", 7421))
        .build();

PrimosMetrics metrics = PrimosMetrics.create(registry, cluster);

PrimosNode node = PrimosNode.builder()
        .clusterConfig(cluster)
        .statsListener(metrics)
        .store(() -> new FileStateStore(directory))
        .transport(UdpTransport.builder()
                .bindAddress(new InetSocketAddress("127.0.0.1", 0))
                .peers(cluster.allPeers()))
        .build();

node.start();
metrics.bind(node);
```

`bind` also registers for role, leader and peer events, which is why there is nothing to pass to `listener(...)`. Bind
it once: a second call is refused, because it would count every event twice.

The examples on this page are compiled and run by
[`ReadmeExamplesTest`](../primos-micrometer/src/test/java/org/lolaf/primos/micrometer/ReadmeExamplesTest.java).

## The one meter to alert on

**`primos.leader`**, a 0/1 gauge per node. Summed across a cluster it is `1` when things are well, `0` during an
election, and `2` when this library has failed at its one job.

```
sum by (application) (primos_leader) != 1
```

Everything else on this page is diagnosis after that alert has fired.

## Meters

### Polled from the node

| meter | type | what it says |
|---|---|---|
| `primos.leader` | gauge | 1 while this node leads |
| `primos.role` | gauge | 1 for the role held, tagged `role` |
| `primos.term` | gauge | the term this node is in, which is the fencing token |
| `primos.node.running` | gauge | 1 while started: separates a node nobody started from a healthy follower |
| `primos.candidate` | gauge | 1 while this node stands for election; summed across a cluster, 0 means nobody can take over |
| `primos.voters` | gauge | voters in the configured cluster |
| `primos.quorum.required` | gauge | votes needed to lead |
| `primos.peer.up` | gauge | 1 while a peer answers probes |

`primos.voters` and `primos.quorum.required` never move, and earn their place anyway: against the peers that are up
they say how close the cluster is to having no leader at all.

### Pushed as they happen

| meter | type | what it says |
|---|---|---|
| `primos.campaign` | timer | how long a campaign took, tagged `outcome` = `won`/`lost` |
| `primos.leaderless` | timer | how long this node had no leader, spanning the whole outage |
| `primos.leadership.relinquished` | counter | stand-downs for want of contact with a majority |
| `primos.handover` | counter | leadership handed over, tagged `direction` = `sent` (shutting down), `transferred` (staying), `accepted` |
| `primos.handover.no.successor` | counter | transfers that found no follower standing for election |
| `primos.probe.rtt` | timer | every probe round trip, per peer |
| `primos.datagrams.dropped` | counter | drops, tagged `reason` |
| `primos.peer.down` | counter | times a peer stopped answering |
| `primos.departures.observed` | counter | peers that announced a shutdown |
| `primos.timing.mismatches` | counter | peers running different intervals from this node |
| `primos.node.config.unusable` | counter | peers stating intervals that form no valid profile |
| `primos.peers.forgotten` | counter | learned voters dropped after going quiet; zero on a node |
| `primos.role.changes` | counter | role transitions, tagged `to` |
| `primos.leader.changes` | counter | leader changes seen, including losing one |
| `primos.versions.published` | counter | versions this node published as leader; several between two heartbeats count once |
| `primos.versions.received` | counter | leader versions this node was told about as a follower |

Three of these answer questions no snapshot could. `primos.campaign` and `primos.leaderless` are durations that begin
and end between two scrapes, the two numbers most wanted after a failover. `primos.probe.rtt` carries every sample,
which is what a percentile needs. And `primos.leader.changes` catches leadership going A to B to A inside one scrape
interval, which is exactly the flap worth alerting on.

**There is no counter beside a timer.** A timer publishes its own count, so `primos.campaign{outcome="won"}`'s count
*is* elections won.

`primos.departures.observed` against `primos.peer.down` is worth a panel: departures that were announced are orderly
shutdowns, and the rest were inferred from silence (a crash, a partition, or a garbage collection long enough to look
like one).

## Tags

Every meter carries **`application`** and **`node`**. One JVM runs several nodes in tests and one node per served
cluster in a witness, so untagged meters would silently sum across clusters.

Meters about one peer also carry **`peer`** and **`peer.type`**. The peer tag is resolved from the configured peer set
when you bind, and:

- an id outside that set is bucketed as `peer=other`;
- a **witness server**, which learns its voters from traffic rather than from configuration, collapses every
  peer-scoped meter to `peer=learned`.

That is a bound rather than a convenience. Learned ids arrive on a socket, so tagging per peer would be a label set
chosen by whoever is sending: the metrics version of a memory leak an attacker controls.

Separating `peer.type` matters more than it looks: a witness sits in a third failure domain, so its round trip is a
WAN number that should not share a histogram with the LAN ones.

**Percentiles are yours to switch on.** Nothing here configures a histogram; use a `MeterFilter` on your registry.
Recording into one costs allocation on the node thread, and this module is measured at zero bytes an event.

## A witness server

Same two halves, and for the same reason:

```java
WitnessMetrics metrics = WitnessMetrics.create(registry, nodeId);

WitnessServer server = WitnessServer.builder()
        .configStore(configStore)
        .nodeId(nodeId)
        .statsListeners(metrics.statsListeners())
        .build();

metrics.bind(server);
```

Every served cluster gets the whole node table above, tagged with its own name. The witness's own meters are:

| meter | type | what it says |
|---|---|---|
| `primos.witness.routing` | function counter | datagrams that reached no application, tagged `reason` |
| `primos.witness.applications` | gauge | clusters served, tagged `state` = `served`/`quarantined` |
| `primos.witness.term` | gauge | the term held for one served cluster |
| `primos.witness.quarantined` | gauge | 1 while a served cluster's node has failed |

The routing meters carry `node` and **no `application`**: the meaning of `unroutable` is precisely that the datagram
could not be attributed to a cluster.

Alert on **`primos.witness.routing{reason="undeliverable_replies"}`**. It should stay at zero, and a witness that
silently fails to answer is indistinguishable from a witness nobody is asking, while the cluster on the other end waits
out an election timeout for a vote that was never coming.

`awaiting_node_config` is ordinary and transient while a new cluster starts, and means something else when it is
sustained: a cluster is talking to this witness and never saying who it is.

## Writing your own

The binder never blocks and never allocates on the node thread, because it is called from the election loop. If you
write your own `PrimosStatsListener`, hold that contract: resolve your meters once, up front, and increment them.
`registry.counter(name, tags)` inside a callback allocates on the one path whose rate an attacker chooses.
