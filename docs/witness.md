# Witness manual

A **witness** is a voter that never becomes leader. One witness process serves many clusters at once, keyed by
application name: one socket, one thread, and an ordinary voter per served cluster. Why a cluster wants one is in
[Cluster sizing and the witness](cluster-sizing.md).

This manual describes the witness itself, so everything in it holds whichever way you run it. Installing one is
covered separately: [from the tarball under systemd](witness-cli.md), or
[as a Spring Boot application and container image](witness-spring-boot.md). The tarball ships this page as
`MANUAL.md`.

## Three rules that are safety, not advice

Break any of these and the witness causes the split brain it was deployed to prevent.

### 1. Never run two instances for one identity

No virtual address, no load balancer, no DNS round-robin, no "let's run two for redundancy". Two witnesses answering
for one cluster is **a double vote by construction**: each grants its own vote in the same term and two nodes each
believe they have a majority.

If you need the witness itself to be highly available, the answer is that you do not. Losing it returns the cluster to
the fault tolerance it had without one, which is the state you were in before you deployed it. That is a degradation,
not an outage.

### 2. Never wipe the state directory

Not an ephemeral container volume, not a `tmpfs`, not "clear it and restart". The witness fsyncs
`currentTerm`/`votedFor` per application **before** answering any vote. A witness that forgets a vote and restarts can
vote twice in one term and produce two leaders.

It refuses to start on a store that reports itself non-durable, rather than warning.

Removing an application's configuration stops it being served but **leaves its state on disk**, deliberately: a
cluster that comes back later still needs the vote it was given remembered. A witness on `config.source=cluster`
therefore accumulates state until an operator cleans it up.

### 3. Never run without a secret

Authentication protects **replies** as much as requests. One forged vote grant, plus the candidate's own vote, is a
majority in a three-voter cluster, so an unauthenticated witness link is a direct route to a false leader, not merely a
denial-of-service risk.

It is also the only admission control the witness has. A node checks that a sender is one of its configured peers; the
witness cannot, because it learns its voters from traffic. **The tag is the whole of the gate.**

Without a secret the witness is also a small reflector: spoof a source address and it replies there. It is not an
amplifier, since every reply is smaller than its request, and with a secret set it cannot be provoked at all.

## The node id is durable

The witness's node id is recorded in every served application's `votedFor`, and every cluster must configure the same
value as its witness peer's id:

```java
.witness(Peer.witness(1023, "witness.example.com", 7420))
```

Changing it on a running witness makes its recorded votes unattributable. Pick it once. The default is `1023`.

## A clean stop is worth waiting for

On `SIGTERM` the loop finishes its turn, tells every served cluster it is going away so none of them waits out a probe
timeout, and closes its state stores before exiting. Give it the time to do that (systemd's `TimeoutStopSec`, Docker's
`--stop-timeout`, Kubernetes' `terminationGracePeriodSeconds`) rather than following up with `SIGKILL`.

## Firewall

One inbound UDP port (7420 by default) from every node of every served cluster. Replies leave from the same socket, so
no separate outbound rule is needed, and that is also what keeps NAT pinholes open in both directions.

## Configuring

A directory, not a single file:

```
/etc/primos-witness/
├── witness.properties
└── applications/
    ├── orders.properties
    └── billing.properties
```

Under `config.source=file` the split earns its keep on shared infrastructure: adding a cluster is a **file drop**
rather than an edit to a file every tenant contends over.

**Under `config.source=cluster` there is no per-tenant file to drop, and `applications/` need not exist at all.**
Applications are created by the config messages their nodes send, timings come from those messages, and keys live in
the keyring, so a witness in that mode needs exactly one file. This is also the shape that fits a single Kubernetes
ConfigMap, since a ConfigMap key cannot contain `/`.

### `witness.properties`

| key | default | hot-reloadable |
|---|---|---|
| `bind.host` | `0.0.0.0` | **no**: read and ignored until restart |
| `bind.port` | `7420` | **no** |
| `state.directory` | `<config>/state` | **no** |
| `config.source` | `file` | yes |
| `config.cluster.limit` | `64` | yes |
| `secret.<name>.value` | none | yes |
| `secret.<name>.algorithm` | `HmacSHA256` | yes |
| `secret.<name>.applications` | none: claims nothing | yes |
| `timings.*` | see below | yes, and read but ignored under `config.source=cluster` |
| `vote.logging` | `true` | **no**: read at startup |
| `reload.rescan.seconds` | `5` | yes |

Keys marked not hot-reloadable are read and ignored with a log line, rather than half-applied.

#### Timings

`timings.profile` is `lan` or `wan`: a named profile rather than several separate intervals, because it is the
*relationships* between them that have to hold. A witness genuinely on a third network wants `wan`.

**Set the same profile on the witness and on every node of the clusters it serves.** A node refuses to grant a
pre-vote while it has heard from a leader within `electionTimeoutMin`, so a `wan` witness among `lan` nodes is five
times more reluctant to agree to a failover than its peers, and for a two-node pair the witness's vote is the one
everything is waiting on. The full derivation, including the garbage-collection floor, is in [Timings](timings.md).

If the clusters you serve run neither profile, spell the intervals out instead, as whole milliseconds, because this
file is parsed with `java.util.Properties` and nothing else:

```properties
timings.heartbeat.millis=250
timings.election.min.millis=2000
timings.election.max.millis=4000
timings.probe.millis=3000
timings.advertisement.millis=60000
timings.failure.threshold=5
```

**A profile and an individual interval together are refused**, naming both. The two election bounds are a pair and
must be set together, and every relationship between them is validated at load: a witness that would thrash refuses to
start rather than doing it in production.

**Anything omitted falls back to the LAN value, never to a profile's.** A witness on a third network that sets only
`timings.probe.millis` keeps a 50ms heartbeat and a 300 to 600ms election timeout, which nothing will object to and
which is wrong for a WAN round trip. Name every value, or name the profile.

Timings in `witness.properties` are the *default* for tenants that say nothing. **Set them per application whenever you
serve more than one cluster**: a witness serving a LAN cluster and a WAN one has to match both, and one witness-wide
value cannot. Nodes advertise their intervals and compare them, so the mismatched tenant would log an error its
operator has no way to resolve. `config.source=cluster` is the other answer to the same problem: each tenant's own
nodes become the source, so there is nothing here to keep in step.

**Under `config.source=cluster` none of the timing keys apply.** Each cluster states its own intervals and this
witness follows them, so whatever is written here is read, parsed, and then replaced by the first thing that cluster's
nodes say. That is logged once per application rather than ignored quietly. There is no merge of the two.

Changing timings never needs a restart: a reconfigured application has its node rebuilt, keeping its state store and
its learned addresses.

#### Vote logging

`vote.logging` logs who voted for whom at INFO instead of DEBUG, for every application this witness serves. It is on by
default and it is worth more here than anywhere else: the witness holds the vote that breaks the tie, so *did the
witness grant it, and to whom?* is the first question asked after a two-node pair failed over (or failed to), and
nothing else records the answer. It costs nothing while leaders are healthy, because a cluster that is not electing
never reaches the code that logs. Turn it off for a deployment that elects often enough for the lines to be noise; the
decisions are still there at DEBUG.

Each line carries its application name, so one cluster's election is greppable out of the rest.

### `applications/<name>.properties`

**The application name is the filename.** `orders.properties` serves the cluster named `orders`, so the name cannot
disagree with the file's own contents. It is validated against the wire's rules at load (printable ASCII, no spaces, at
most 32 bytes) and rejected rather than silently truncated.

| key | notes |
|---|---|
| `timings.profile` | `lan` or `wan`; falls back to the witness-wide value |
| `timings.heartbeat.millis` | `50` by default |
| `timings.election.min.millis` | `300` |
| `timings.election.max.millis` | `600`; set with its pair |
| `timings.probe.millis` | `1000` |
| `timings.advertisement.millis` | `30000`; never shorter than the probe interval |
| `timings.failure.threshold` | `3` |

An empty file is valid and means "serve this cluster, with whatever key the keyring says covers it". **Key material
here is refused, not ignored**; see [Keys](#keys) for where it goes and why.

The timings must match what that cluster's nodes run, with the same rules as in `witness.properties`. Under
`config.source=cluster` they are ignored and the cluster's own nodes decide. A mismatch is logged at ERROR by both
sides, because nodes advertise their intervals.

### Keys

Every key this witness holds lives in `witness.properties`, never in an application's own file:

```properties
secret.platform.value=file:/etc/primos-witness/keys/platform
secret.platform.algorithm=HmacSHA256
secret.orders.value=env:ORDERS_KEY
secret.orders.applications=orders-*, *-test
```

**Why witness-wide rather than per application.** A cluster that arrives over the network has no file to have had a
secret set in. Keeping keys here makes them work identically under both values of `config.source`, and it is why an
application file carrying `secret` is *refused* rather than ignored: an operator believing a tenant has its own key
while the keyring decides otherwise is the worst failure available here, so the witness will not start.

`value` takes four forms, so configuration can go into version control while the key does not: inline, `env:NAME`,
`file:/path`, `base64:...`. The loader warns about world-readable files the way `ssh` does; keep them `0640` and owned
by the service user.

`algorithm` is per key, because a witness serves clusters it does not own and one of them moving to another algorithm
must not be a flag day for the rest. **It has to match what that cluster's nodes were built with.** Nothing on the wire
says which algorithm produced a tag, so a mismatch looks exactly like a wrong key: every datagram fails verification
and is counted under `BAD_TAG`. Almost nobody should set it.

#### Which key may speak for which application

One rule: **a name that any key claims is verified only by the keys claiming it; a name nothing claims is verified by
the keys that claim nothing.**

A claim is a *reservation*, not merely a restriction, and that is the whole of what makes it worth having. Were a
scoped key only limited to its names, a general key beside it would still verify those names too, and a tenant with
its own key would gain nothing.

`applications` is a comma-separated list of patterns. `*` is the only metacharacter and matches any run of characters,
so `orders-*` covers a family without naming its members. **These are not regular expressions**: as one, `orders-*`
would mean "orders followed by zero or more hyphens". A bare `*` means the same as leaving the setting out, so it
cannot silently take every unscoped key out of use.

Two keys covering one name, through overlapping patterns or because nothing claims it, means either key verifies it.
That is not detected. Do not overlap.

| keyring | what you get |
|---|---|
| every key unscoped | any cluster may arrive, and **every keyholder can speak for every other**: one team, many clusters, one template |
| some scoped | a general key for the fleet, dedicated keys for the tenants that matter |
| every key scoped | only the named families can be served at all, and no keyholder can reach another's names |

The last row is a legitimate lockdown, and under `config.source=cluster` it is also how you get an explicit tenant list
while their timings still come from their own nodes: the scope lists *are* the tenant list. It is indistinguishable
from a witness that is quietly broken, so the startup log says which it is:

```
witness 1023: 3 key(s), all scoped: only names matching orders-*, billing-*, *-test can be served
```

### Rotating a key

Rotation is list membership. No restart, no flag day:

1. Add the new key beside the old one. The witness now accepts either, and signs each reply with whichever key
   verified the request, so a node holding only one of them still hears an answer.
2. Roll the cluster's nodes onto the new key at your own pace.
3. Remove the old entry. It stops working.

Do not skip step 3. Two keys is a rotation window, not a permanent second key.

### Hot reload

The configuration directory is re-read while the witness runs. **This is the point of the design**, because
restarting a witness is the one genuinely risky operation in its lifecycle: while it is down, every two-node cluster it
serves is back to tolerating zero failures.

- **Every few seconds the configuration is checksummed**, and nothing further happens unless a file has actually
  changed. There is no file watcher.
- **There used to be one, and it was deleted because it could stop working silently.** On a Kubernetes ConfigMap the
  volume is a symlink farm: an update writes a new timestamped directory, swaps `..data` atomically and deletes the old
  one, so a watch registered on the resolved directory is watching something deleted. It never fires again and nothing
  says so. A check that always runs is worth more than a notification that might have stopped.
- **A malformed file never takes down the running configuration.** The candidate is parsed and validated whole, and
  published only if it is good; otherwise the last good configuration keeps serving and the failure is logged loudly.

## Where configuration comes from: `config.source`

### `file` (default)

Serves exactly the applications configured in `applications/`, and drops everything else, counted and logged once per
unseen name. Nothing arriving over the network changes any configuration here. This is the mode for a deployment whose
operator has a hard constraint against automatic change.

### `cluster`

Serves any application name it hears, creating state for it. Less operational burden, a legitimate choice, and the way
a typo becomes a split brain.

**Only a node stating its own configuration creates an application, and only if a key that may speak for that name
accepts it.** A `Ping`, a `RequestVote` or a heartbeat naming an application this witness does not serve is dropped and
counted under `awaitingNodeConfig`; nothing is created until one of that cluster's nodes sends a
[`NodeConfig`](protocol.md#nodeconfig-0x09). That is ordinary and brief while a cluster starts, since its first probes
and votes arrive before its first config message, and it stops the witness ever serving a tenant it has no timings for.
Sustained, it means a cluster is talking to this witness and never saying who it is.

A config message that no covering key accepts is dropped and counted under `registrationBadTag`, and **nothing is
created**. That bounds who may make state directories here: reaching the port is not enough, and neither is holding
some *other* tenant's key. A witness configured with no keys at all admits whatever arrives, which is one more reason
rule 3 says never to run one.

**Its precondition, which is easy to miss:** a cluster-created application has no file, so a key in the keyring has to
cover it, either an unscoped one or one whose pattern matches its name. Nothing else is needed: its timings come from
its own nodes.

**Keys are the one thing that can never come from the cluster.** A key authenticates the datagram that would have
carried it, so taking one over the wire is circular, and the algorithm is deliberately absent from the wire too. How
much isolation you get is decided by your key patterns, not by `config.source`.

| differs | what happens |
|---|---|
| the key or its algorithm | every datagram fails verification, counted under `BAD_TAG`. Total, and obvious. Add a key covering that cluster |
| timings | nothing. The witness follows what that cluster's nodes state, per application, and rebuilds when they change |

**The walkthrough that is the reason `file` is the default:**

> Two clusters are deployed. One is configured `orders`. The other, through a typo in a config file, is configured
> `ordres`.
>
> The two sets of nodes drop each other's datagrams, because the application name does not match: the cross-talk
> protection working exactly as designed. So each side sees the other as unreachable, and each asks the witness for a
> vote.
>
> A witness on `config.source=cluster` auto-creates **two** state buckets, one per name, and grants a vote from each.
> Both sides now have a majority. **Two leaders, out of one typo**, and the very protection that should have caught the
> mismatch is what hides the cause, because neither cluster ever sees the other's traffic.

`cluster` is hardened rather than merely documented:

- every auto-registration logged at WARN with the name and source address;
- **near-miss detection** on first sight of a name (case-insensitive, small edit distance against names already
  served), warning explicitly that it may be a typo. It also runs under `file`, where a dropped `ordres` next to a
  served `orders` is the entire diagnosis in one line;
- `config.cluster.limit`, so an open witness cannot be filled up by whoever can reach the port;
- state is **never** auto-expired, because deleting `votedFor` for a dormant cluster that later returns is precisely
  the double-vote bug.

A witness does forget *voters* it learned from traffic once they go silent. That is safe only because it has no quorum
of its own to shrink: it never campaigns and never leads.

## Operating

### What it logs

At startup:

```
serving 2 application(s): billing, orders
configuration source is FILE
witness 1023 bound to /0.0.0.0:7420, configuration from FILE, state in /var/lib/primos-witness
cluster 'orders': witness 1023 is serving, with the voter set learned from traffic
witness 1023 is serving 2 application(s)
```

Worth alerting on:

| level | message | means |
|---|---|---|
| WARN | `no applications configured` | `config.source=file` with an empty `applications/` serves nobody. Under `cluster` this is INFO, because empty is the intended shape |
| WARN | `no keys are configured` | see rule 3 |
| WARN | `... is world-readable and may contain a cluster secret` | fix the file mode |
| WARN | `dropping traffic for unserved application 'X'` | a cluster is asking and being refused |
| WARN | `'X' is one or two characters from 'Y'` | **near miss: check both configurations** |
| WARN | `auto-registering application 'X'` | `config.source=cluster` serving someone unconfigured |
| WARN | `refusing to auto-register` | `config.cluster.limit` reached |
| WARN | `no key that may speak for that name accepts it` | a cluster holds a key this witness does not, or somebody holding none is trying to create applications |
| WARN | `stated intervals that do not hold together` | a cluster advertised a profile that could not be built; the last good one stays in force |
| INFO | `Holding off until one of its nodes states its configuration` | ordinary while a cluster starts; sustained means it never says who it is |
| ERROR | `cannot read the configuration in ...` | a bad edit; the last good config is still serving |
| ERROR | `application 'X' has failed` | that cluster is no longer served; see below |

With metrics, the counters worth alerting on are in [Micrometer](micrometer.md#a-witness-server).

### One application failing does not stop the witness

If an application's state store fails (a full or broken disk), that application is **quarantined**: it stops being
served, and every other tenant keeps working. One team's storage problem must not be every team's failover outage.

The clusters relying on that application's witness are back to the fault tolerance they have without one. Fix the
cause and restart the witness.

### State on disk

```
/var/lib/primos-witness/
├── orders/{state,state.lock}
└── billing/{state,state.lock}
```

A directory per application, so one cluster's state can be backed up, moved or removed on its own, and two
applications cannot share a file lock and thereby a failure. `state.lock` is an exclusive lock: a second witness
pointed at the same directory fails to start rather than quietly double-voting.

Back it up if you like, but **never restore an old copy**: restoring a stale `votedFor` is indistinguishable from
forgetting a vote. If in doubt, an empty state directory for a *new* witness identity is safer than a stale one for an
existing identity.

### Removing an application

Delete `applications/<name>.properties`. It stops being served within one reload cycle. Its state directory stays;
remove it by hand only once you are certain the cluster is gone for good.

## Troubleshooting

**A cluster cannot reach the witness.** Its nodes log `the witness (N) is unreachable`. Check UDP reachability on the
port first, then that the witness logs the application as served, then that the node ids match: the witness's own node
id must equal the id each cluster configures with `Peer.witness(id, ...)`.

**The witness sees the traffic but never answers.** Almost always the secret. A datagram failing authentication is
dropped silently before anything is parsed, by design, so look at the drop counters rather than for an error. Check
that some key in the keyring covers that application (a name claimed by a key the cluster does not hold is verified by
nothing else), and check for a rotation where the node holds only a key already removed from the ring.

**`dropping traffic for unserved application 'X'`.** `config.source=file`, and no `applications/X.properties` exists.
If `X` looks like a near miss of something you do serve, treat it as a typo in that cluster's configuration and not as
something to fix by adding a file.

**Two leaders in one cluster.** Check, in order: two witness instances answering for the same identity (rule 1); a
wiped or restored state directory (rule 2); two clusters whose names differ by a typo on a witness with
`config.source=cluster` (the walkthrough above).

**Failover is slower than expected.** The witness's intervals should match the deployment: `wan` if it sits on another
network, or `config.source=cluster` so the tenant's own nodes settle it. Both sides log an ERROR naming the two interval
sets when they disagree. The witness does not campaign, so it never causes an election; it only answers.

**A cluster is talking to the witness but is never served.** Under `config.source=cluster` only a node stating its
configuration creates an application; look for `Holding off until one of its nodes states its configuration`. Under
`config.source=file`, it needs `applications/<name>.properties`.

## What it does not do

No replication, no state machine, no snapshots, no runtime membership changes. The witness never campaigns, never
becomes leader, and never appears as one to any listener. It votes, and that is all.
