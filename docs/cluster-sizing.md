# Cluster sizing and the witness

## Voters and quorum

A cluster of *n* voters needs a majority, `n / 2 + 1`, to elect a leader and to keep one. **Quorum comes from the
configured membership, never from who is reachable**: five voters with three gone still need three votes. Reading a
peer-down event as "the cluster shrank" is how a split brain is talked into existence.

| configuration | voters | quorum | tolerates |
|---|---|---|---|
| 1 node | 1 | 1 | 0, and see below |
| 2 nodes | 2 | 2 | **0** |
| 2 nodes + witness | 3 | 2 | 1 |
| 3 nodes | 3 | 2 | 1 |
| 3 nodes + witness | 4 | 3 | **1** |

Every node logs this summary for its own cluster at startup, plus a warning when the configuration is strictly
dominated by an alternative.

## Why a pair needs a witness

A two-node active/passive pair is the case primos exists for. Two nodes alone form a 2-voter cluster with quorum 2,
which tolerates **zero** failures: any death or partition leaves no leader at all. The pair looks redundant and is not.

A **witness** is a voter that never campaigns and never leads, deployed in a third failure domain. It makes the pair 3
voters with quorum 2: whichever node still reaches the witness has a majority and leads, the other stands down, and a
partition gets a winner and a loser instead of two winners.

It is an ordinary voter that declines to campaign. It sits in the voter set, persists its term and vote, and answers
`RequestVote` like any node. That is deliberately the boring choice: it stays inside Raft's existing correctness
argument rather than bolting a second consensus mechanism onto the first.

**"Third failure domain" is the whole point.** A witness on the same host, rack, power feed or availability zone as
one of the nodes buys very little: the failure that takes out that domain takes out the majority with it.

A witness helps even-sized clusters and hurts odd-sized ones: three nodes plus a witness need three of four processes
alive instead of two of three. That is suboptimal, not unsafe, so primos warns and continues rather than refusing.

Each node of the cluster names the witness beside its peers, with the id the witness runs under:

```java
ClusterConfig.builder()
        .applicationName("orders")
        .nodeId(1)
        .peer(Peer.application(2, "node-b", 7420))
        .witness(Peer.witness(1023, "witness.example.com", 7420))
        .build();
```

One witness process serves many clusters, keyed by application name. Running it is covered by the
[witness manual](witness.md).

## One node leads itself, permanently

**A node configured with no peers is a one-voter cluster, and it becomes the leader.** Quorum is one, so it elects
itself about an election timeout after starting, and then nothing can depose it: there is no majority for it to lose
contact with, so the check that stands a partitioned leader down never fires.

That is deliberate and supported. It makes development and testing trivial, and it lets an application run the same
code path in a single-instance deployment instead of branching around primos entirely.

**It is also exactly what a peer list that failed to load looks like**: a mistyped property, an environment variable
that did not interpolate, a YAML block at the wrong indentation. Made once, you get a node that quietly leads alone.
Made on every node of an intended cluster, you get one permanent leader per node, each reporting perfect health and
each right about its own arithmetic. Nothing in the protocol can detect this, because from inside, a one-node cluster
is behaving correctly.

So primos warns at startup whenever it is configured this way:

```
cluster 'orders': 1 voter (1 node), quorum 1, tolerates 0 failures
cluster 'orders': one node and no witness, so this node is the only voter. It elects itself about
one election timeout after starting and leads for as long as it runs, and nothing can depose it —
there is no majority to lose contact with. That is a legitimate single instance deployment, and it
is also what a mistyped or unread peer list looks like, in which case every node of the intended
cluster is doing this at once and each of them is the leader. Check that peers were meant to be
omitted.
```

If you meant it, the warning is one line at startup. If you did not, it is the only thing that will tell you.
