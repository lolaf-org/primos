# Leader data

A leader often owns some data the other nodes need: a shard assignment, a routing table, a schedule. Primos does not
carry that data, but it tells every node when the leader's data has changed, so each can fetch it from the leader over
your own channel (HTTP, gRPC, anything).

The leader calls `publishChange()`. Primos puts a version number in every heartbeat, and each node is told through
`onLeaderVersion` when it sees a newer one. The data never touches primos, so it can be any size.

## The leader publishes

```java
Optional<PublishedVersion> published = node.publishChange();
```

`publishChange()` returns the term and version it published, or empty when this node is not leading. Store both with
the data you serve, and serve them together:

```java
final class Edition {
    final long term;
    final long version;
    final Assignment assignment;

    Edition(long term, long version, Assignment assignment) {
        this.term = term;
        this.version = version;
        this.assignment = assignment;
    }
}

class AssignmentPublisher implements PrimosListener {

    private final AtomicReference<Edition> served = new AtomicReference<>();
    private final ExecutorService work = Executors.newSingleThreadExecutor();
    private PrimosNode node;

    void attachTo(PrimosNode node) {
        this.node = node;
        node.addListener(this);
    }

    @Override
    public void onRoleChanged(Role previous, Role current, long term) {
        if (current == Role.LEADER) {
            work.submit(this::rebalance);   // never on the node thread
        }
    }

    void rebalance() {
        Assignment next = Assignment.compute();
        node.publishChange().ifPresent(published ->
                served.set(new Edition(published.term(), published.version(), next)));
    }

    Edition current() {             // what your endpoint returns
        return served.get();
    }
}
```

Call `rebalance()` again whenever the assignment changes. Every call publishes a new version.

## Followers fetch

Every node is told, the leader included: `leader.isSelf()` says there is nothing to fetch.

```java
class AssignmentFollower implements PrimosListener {

    private final ExecutorService work = Executors.newSingleThreadExecutor();
    private final Fetcher fetcher;              // your HTTP or gRPC client
    private volatile Edition applied;

    AssignmentFollower(Fetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void onLeaderVersion(Leader leader, long term, long version) {
        if (leader.isSelf()) {
            return;
        }
        leader.peer().ifPresent(at -> work.submit(() -> fetch(at, term, version)));
    }

    void fetch(Peer leader, long term, long version) {
        Edition edition = fetcher.fetch(leader.host());
        if (edition == null || edition.term != term || edition.version < version) {
            return;                             // not there yet, or leadership moved on: retry or wait
        }
        applied = edition;
    }
}
```

**Check the term of what you fetched.** Leadership can change between the heartbeat and the fetch, and a deposed
leader can still answer one. Data whose term is not the one you were told about is not the current leader's.

**The fetch can race the publication.** The leader stores its data just after `publishChange()` returns, so a very
fast follower may fetch the previous edition. Checking `edition.version < version` catches it; retry after a short
delay.

## What you are told, and when

- **Once per change, never per heartbeat.** Heartbeats repeat the version 20 times a second on the LAN profile;
  you are told only when it is newer than the last one you were told about.
- **The latest, not every step.** Several `publishChange()` calls between two heartbeats reach followers as the last
  one. Fetching the latest data is the point, so the versions in between are never worth a call.
- **Never for version 0.** A new leader has published nothing until it calls `publishChange()`, and `onLeaderChanged`
  already says who leads. Decide what to do in the meantime: keeping the previous assignment is usually right, as long
  as whatever acts on a shard checks the term.
- **A late or restarted node** is told on the first heartbeat it hears, if the leader has published anything.
- **A lost heartbeat costs nothing**: the next one carries the same version.
- **A witness is never told**: it runs no application.

## Versions restart with each term

The counter is not replicated: a new leader cannot know the number its predecessor reached, so its first publication
is version 1 again. A version alone orders nothing across a leader change. Always store and compare the pair, term
first.

## Using it for sharding

Three rules decide whether an assignment built on this is safe:

- **Fence by term.** Every action taken on a shard carries the term of the assignment it came from, and whatever the
  action touches rejects a term lower than the highest it has seen. For a moment two nodes may each believe they own a
  shard; the term is what makes the stale owner's work fail. See [Using leadership safely](using-leadership-safely.md).
- **Assign over the configured members, not the reachable ones.** `onPeerDown` means unreachable, and on the JVM a
  garbage collection looks the same. Reshuffling on every peer-down event moves shards on every pause.
- **Spread the fetches if there are many nodes.** Every follower is told at about the same moment; with dozens of
  nodes, a small random delay before fetching keeps them off the leader all at once.

## On the wire

The version is an 8-byte field in every `AppendEntries`, so a heartbeat is 92 bytes with the default HMAC; see
[Wire protocol](protocol.md#appendentries-0x03). The election never reads it.
