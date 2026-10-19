# Building a node

A node is built from two objects, because they answer different questions. A `ClusterConfig` says who is in the
cluster; the node builder says how this node runs.

```java
List<Peer> peers = Arrays.asList(
        Peer.application(2, "node-b", 7420),
        Peer.application(3, "node-c", 7420));

ClusterConfig cluster = ClusterConfig.builder()   // who is in the cluster
        .applicationName("orders")
        .nodeId(1)
        .peers(peers)
        .build();

PrimosNode node = PrimosNode.builder()            // and how this node runs
        .clusterConfig(cluster)
        .clusterSecret(secret)
        // Suppliers, not instances: each start binds a socket and takes the state lock, so a
        // node that stops can start again. The transport builder is itself the supplier.
        .store(() -> new FileStateStore(Paths.get("/var/lib/orders/primos")))
        .transport(UdpTransport.builder()
                .bindAddress(new InetSocketAddress(7420))
                .peers(peers)
                .addressLearning(true))       // only ever with a secret set
        .listener(new PrimosListener() {
            @Override
            public void onRoleChanged(Role previous, Role current, long term) {
                log.info("now {} in term {}", current, term);
            }
        })
        .build();

node.start();
```

The `ClusterConfig` is the same shape on every node, and it is where a cluster's shape is validated, once. The node
builder wires this node's runtime around it: where its state lives, how it reaches the network, who listens.

The examples on this page are compiled and run as a test
([`ReadmeExamplesTest`](../primos/src/test/java/org/lolaf/primos/ReadmeExamplesTest.java)), so an API change that makes
them wrong breaks the build.

## Peers

Every node lists the *other* members of the cluster, each with a node id unique within the application. A witness, if
the cluster has one, is added beside them with the id the witness itself runs under:

```java
ClusterConfig.builder()
        .applicationName("orders")
        .nodeId(1)
        .peer(Peer.application(2, "node-b", 7420))
        .witness(Peer.witness(1023, "witness.example.com", 7420))
        .build();
```

**Quorum comes from this configured membership, never from who is reachable.** Five voters with three gone still need
three votes. [Cluster sizing and the witness](cluster-sizing.md) covers how many voters to configure, and what a node
with no peers at all does.

## Listening

Every `PrimosListener` method has a default, so override only what you need: `onRoleChanged`, `onLeaderChanged`,
`onLeaderVersion`, `onPeerUp`, `onPeerDown`. `onLeaderVersion` says the leader's own data changed; see
[Leader data](leader-data.md).

**Callbacks arrive on the node thread.** Hand real work to your own executor, or you are holding up heartbeats. A
callback slower than half the heartbeat interval is logged as such.

What you branch on is a plain call: `role()`, `roleAndTerm()`, `leader()`, `isRunning()`. Read
[Using leadership safely](using-leadership-safely.md) before branching on any of them.

## Store and transport

`store` and `transport` are required and have no defaults. A node that cannot remember its vote across a restart can
elect two leaders, so there is no convenient in-memory fallback to reach for by accident.

`FileStateStore` fsyncs the term and the vote before any message depending on them is sent. Put its directory on local,
durable storage: a network filesystem or a throttled cloud volume whose worst-case sync approaches the election timeout
will lose elections while writing.

`addressLearning` lets a node follow a peer whose address changes. Enable it only with a cluster secret set: without a
tag, one spoofed datagram would redirect a peer's traffic and partition it without forging any content.

## Authentication

`clusterSecret` turns on an HMAC on every datagram, verified before anything is parsed. HMAC-SHA256 is the default, and
`.macAlgorithm("HmacSHA512")` on the builder chooses another. Every member of a cluster, the witness included, must hold
the same secret and algorithm: nothing on the wire names either, so a mismatch looks exactly like a wrong key and every
datagram is dropped.

## Stopping and starting again

**A node can leave the cluster and rejoin.** `stop()` then `start()` works, because the builder takes *suppliers* of
the store and transport and each start makes a fresh pair. A `FileStateStore` takes its exclusive lock in its
constructor and a `UdpTransport` binds its socket in its own, so a stopped node holds a released lock and a closed
channel, and restarting needs new instances rather than a reset.

Rejoining is safe because the term and the vote are on disk: a node that comes back re-reads both and cannot forget a
vote it granted.

A planned `stop()` also hands leadership over and announces the departure; see [Clean shutdown](clean-shutdown.md).

## `FAILED` is terminal

A node whose store can no longer make a vote durable stops claiming leadership permanently, because continuing would
mean voting on state it may forget. A `FAILED` node cannot be restarted. Treat the event as a signal to restart the
process, not to wait.
