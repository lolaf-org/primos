# Primos Witness Server

A **witness** is a voter that never becomes leader. This is the standalone process that provides
one to many clusters at once.

Operator-facing. For the library, see the [project README](../README.md); for the reasoning behind
every decision here, [PLAN.md](../PLAN.md).

---

## Why you would run one

Two nodes alone form a 2-voter cluster with quorum 2, which tolerates **zero** failures. Any death
or partition leaves no leader at all — an active/passive pair looks redundant and is not.

A witness in a **third failure domain** makes it 3 voters with quorum 2. Whichever node still
reaches the witness has a majority and leads; the other stands down. A partition gets a winner and
a loser instead of two winners.

| configuration | voters | quorum | tolerates |
|---|---|---|---|
| 2 nodes | 2 | 2 | **0** |
| 2 nodes + witness | 3 | 2 | 1 |
| 3 nodes | 3 | 2 | 1 |
| 3 nodes + witness | 4 | 3 | **1** |

It helps even-sized clusters and hurts odd-sized ones — 3 nodes + witness needs three of four
processes alive instead of two of three. That is suboptimal, not unsafe, so primos warns and keeps
going. Every cluster logs a quorum summary at startup.

**"Third failure domain" is the whole point.** A witness on the same host, rack, power feed or
availability zone as one of the nodes buys you very little: the failure that takes out that domain
takes out the majority with it.

---

## Three rules that are safety, not advice

Break any of these and the witness causes the split brain it was deployed to prevent.

### 1. Never run two instances for one identity

No virtual address, no load balancer, no DNS round-robin, no "let's run two for redundancy". Two
witnesses answering for one cluster is **a double vote by construction** — each grants its own vote
in the same term and two nodes each believe they have a majority.

If you need the witness itself to be highly available, the answer is that you do not: losing it
returns the cluster to the fault tolerance it had without one, which is the state you were in
before you deployed it. That is a degradation, not an outage.

### 2. Never wipe the state directory

Not an ephemeral container volume, not a `tmpfs`, not "clear it and restart". The witness fsyncs
`currentTerm`/`votedFor` per application **before** answering any vote. A witness that forgets a
vote and restarts can vote twice in one term and produce two leaders.

It refuses to start on a store that reports itself non-durable, rather than warning.

Removing an application's configuration stops it being served but **leaves its state on disk**,
deliberately: a cluster that comes back later still needs the vote it was given remembered. A
witness on `config.source=cluster` therefore accumulates state until an operator cleans it up.

### 3. Never run without a secret

Authentication protects **replies** as much as requests. One forged vote grant, plus the
candidate's own vote, is a majority in a three-voter cluster — so an unauthenticated witness link
is a direct route to a false leader, not merely a denial-of-service risk.

It is also the only admission control the witness has. A node checks that a sender is one of its
configured peers; the witness cannot, because it learns its voters from traffic. **The tag is the
whole of the gate.**

Without a secret the witness is also a small reflector: spoof a source address and it replies
there. It is not an amplifier — every reply is smaller than its request — and with a secret set it
cannot be provoked at all.

---

## Installing

Unpack the distribution wherever you keep such things:

```
primos-witness-<version>/
├── bin/primos-witness
├── lib/*.jar
├── conf-sample/                 # heavily commented; copy it, do not point at it
│   ├── witness.properties
│   └── applications/example.properties
└── LICENSE.txt
```

```sh
install -d -m 0750 /etc/primos-witness/applications
install -d -m 0750 /var/lib/primos-witness
cp -r primos-witness-*/conf-sample/* /etc/primos-witness/
```

`conf-sample/` is deliberately a *sample* rather than the live path, so that upgrading never
overwrites `witness.properties` — an upgrade that replaced your secrets with placeholders would
take out every cluster the witness serves.

Requires a JRE 11 or newer on `PATH`, or `JAVA_HOME` set.

### Running

```sh
bin/primos-witness --config /etc/primos-witness
```

| option | default | notes |
|---|---|---|
| `--config <directory>` | `/etc/primos-witness` | must exist |
| `--node-id <id>` | `1023` | 0–1023. **Fixed once deployed** — see below |
| `--bind <host:port>` | from configuration | overrides `bind.host`/`bind.port` |
| `-h`, `--help` | | |
| `-V`, `--version` | | which build this box is running |

Exit codes, for whatever supervises it: **0** normal, **2** for a mistyped option or a missing
configuration directory — reported as one sentence and the usage text — and **1** for a failure to
start, which is logged with its cause.

**The node id is durable.** It is recorded in every served application's `votedFor`, and every
cluster must configure the same value as its witness peer's id. Changing it on a running witness
makes its recorded votes unattributable. Pick it once.

Each cluster is configured with the matching peer:

```java
.witness(Peer.witness(1023, "witness.example.com", 7420))
```

### systemd

```ini
[Unit]
Description=Primos witness server
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=primos
Group=primos
ExecStart=/opt/primos-witness/bin/primos-witness --config /etc/primos-witness
Restart=on-failure
RestartSec=5s

# The state directory must survive. Everything else may be locked down.
ReadWritePaths=/var/lib/primos-witness
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

`SIGTERM` is a clean shutdown: the loop finishes its turn, tells every served cluster it is going
away so none of them waits out a probe timeout, and closes its state stores before exiting. Give it
a moment rather than following up with `SIGKILL`.

### Firewall

One inbound UDP port (7420 by default) from every node of every served cluster. Replies leave from
the same socket, so no separate outbound rule is needed, and that is also what keeps NAT pinholes
open in both directions.

---

## Configuring

A directory, not a single file:

```
/etc/primos-witness/
├── witness.properties
└── applications/
    ├── orders.properties
    └── billing.properties
```

The split earns its keep on shared infrastructure: adding a cluster is a **file drop** rather than
an edit to a file every tenant contends over, and each key file can carry its own permissions,
which one shared file makes impossible.

### `witness.properties`

| key | default | hot-reloadable |
|---|---|---|
| `bind.host` | `0.0.0.0` | **no** — read and ignored until restart |
| `bind.port` | `7420` | **no** |
| `state.directory` | `<config>/state` | **no** |
| `config.source` | `file` | yes |
| `config.cluster.limit` | `64` | yes |
| `secret.<name>.value` | none | yes |
| `secret.<name>.algorithm` | `HmacSHA256` | yes |
| `secret.<name>.applications` | none — claims nothing | yes |
| `timings.*` | see below | yes — and read but ignored under `config.source=cluster` |
| `vote.logging` | `true` | **no** — read at startup |
| `reload.debounce.millis` | `250` | yes |
| `reload.rescan.seconds` | `30` | yes |

`timings.profile` is `lan` or `wan` — a named profile rather than several separate intervals,
because it is the *relationships* between them that have to hold. A witness genuinely on a third
network wants `wan`: a 50ms WAN round trip against a 300ms election timeout is only 6×, where Raft
wants an order of magnitude.

**Set the same profile on the witness and on every node of the clusters it serves.** A node refuses
to grant a *pre-vote* while it has heard from a leader within `electionTimeoutMin`, so a `wan`
witness among `lan` nodes is five times more reluctant to agree to a failover than its peers — and
for a two-node pair, the witness's vote is the one everything is waiting on. Nothing is corrupted
by a mismatch; failover just gets slower in a way that is very hard to see from either side.

The full derivation, including the garbage-collection floor that matters more than the network on
a JVM, is in the [project README](../README.md#timing).

**If the clusters you serve run neither profile, spell the intervals out instead** — as whole
milliseconds, because this file is parsed with `java.util.Properties` and nothing else:

```properties
timings.heartbeat.millis=250
timings.election.min.millis=2000
timings.election.max.millis=4000
timings.probe.millis=3000
timings.advertisement.millis=60000
timings.failure.threshold=5
```

**A profile and an individual interval together are refused**, naming both. A profile is a set of
values picked so the relationships between them hold, so overriding part of one leaves a custom set
whose author has not seen the rest. The two election bounds are a pair and must be set together, and
every relationship between them is validated at load — a witness that would thrash refuses to start
rather than doing it in production.

**The timing keys belong in an application's own file.** Setting them in `witness.properties` sets
the *default* for tenants that say nothing. Both are hot-reloadable — the witness-wide value
resolves into every application that does not override it, and a reconfigured application has its
node rebuilt, keeping its state store and its learned addresses.

**Under `config.source=cluster` none of them apply.** Each cluster states its own intervals and this
witness follows them, so whatever is written here is read, parsed, and then replaced by the first
thing that cluster's nodes say. It is logged once per application rather than ignored quietly. Set
`config.source=file` to have this file decide instead — those are the two choices, and there is no
merge of them.

**Set them per application, not witness-wide, whenever you serve more than one cluster.** A witness
serving a LAN cluster and a WAN one has to match both, and one witness-wide value cannot. It is not
merely suboptimal: nodes advertise their intervals and compare them, so the mismatched tenant logs
an error about a disagreement its operator has no way to resolve. `config.source=cluster` is the
other answer to the same problem — it makes each tenant's own nodes the source, so there is nothing
here to keep in step.

**Anything omitted falls back to the LAN value in the table, never to a profile's** — the two cannot
both be set, so when you are naming intervals there is no profile for the rest to come from. A
witness on a third network that sets only `timings.probe.millis` therefore keeps a 50ms heartbeat and
a 300–600ms election timeout, which nothing will object to and which is wrong for a WAN round trip.
Name every value, or name the profile.

`vote.logging` logs who voted for whom at INFO instead of DEBUG, for every application this witness
serves. It is on by default and it is worth more here than anywhere else: the witness holds the vote
that breaks the tie, so *did the witness grant it, and to whom?* is the first question asked after a
two-node pair failed over — or failed to — and afterwards nothing else records the answer. It costs
nothing while leaders are healthy, because a cluster that is not electing never reaches the code
that logs. Turn it off for a deployment that elects often enough for the lines to be noise, or one
whose log ingestion is charged by the line; the decisions are still there at DEBUG.

A witness serving many tenants logs each line with its application name, so one cluster's election
is greppable out of the rest.

### `applications/<name>.properties`

**The application name is the filename.** `orders.properties` serves the cluster named `orders`.
That is deliberate — the name cannot disagree with the file's own contents. It is validated against
the wire's rules at load (printable ASCII, no spaces, at most 32 bytes) and rejected rather than
silently truncated.

| key | notes |
|---|---|
| `timings.profile` | `lan` or `wan`; falls back to the witness-wide value |
| `timings.heartbeat.millis` | `50` by default |
| `timings.election.min.millis` | `300` |
| `timings.election.max.millis` | `600`; set with its pair |
| `timings.probe.millis` | `1000` |
| `timings.advertisement.millis` | `30000`; never shorter than the probe interval |
| `timings.failure.threshold` | `3` |

An empty file is valid and means "serve this cluster, with whatever key the keyring says covers
it". **Key material here is refused, not ignored** — see *Keys* below for where it goes and why.

**The timings must match what that cluster's nodes run**, and under `config.source=file` this is
where to say so when tenants differ. Under `config.source=cluster` these keys are ignored and the
cluster's own nodes decide. A node refuses a pre-vote while it has heard from a leader within its own
`electionTimeoutMin`, so a witness with a longer one is proportionally more reluctant to grant the
vote a failover is waiting on — and for a two-node pair the witness's vote is the one everything
waits on. Nothing is corrupted by a mismatch; failover just gets slower. Since nodes advertise their
intervals, a mismatch is now logged at ERROR by both sides rather than being invisible.

Either a profile or the values spelled out, never both, exactly as in `witness.properties`.
Changing them does **not** need a restart: a reconfigured application has its node rebuilt, keeping
its state store and its learned addresses.

### Keys

Every key this witness holds lives in `witness.properties`, never in an application's own file:

```properties
secret.platform.value       = file:/etc/primos-witness/keys/platform
secret.platform.algorithm   = HmacSHA256

secret.orders.value         = env:ORDERS_KEY
secret.orders.applications  = orders-*, *-test
```

**Why witness-wide rather than per application.** A cluster that arrives over the network has no
file to have had a secret set in, so keys kept per application could offer such a cluster one
shared key or none. Putting them here makes keys work identically under both values of
`config.source` — and it is why an application file carrying `secret` is *refused* rather than
ignored: an operator believing a tenant has its own key while the keyring decides otherwise is the
worst failure available here, so the witness will not start.

`value` takes four forms, so configuration can go into version control while the key does not —
inline, `env:NAME`, `file:/path`, `base64:...`. The loader warns about world-readable files the way
`ssh` does; keep them `0640` and owned by the service user.

`algorithm` is per key, because a witness serves clusters it does not own and one of them moving to
another algorithm must not be a flag day for the rest. **It has to match what that cluster's nodes
were built with** — nothing on the wire says which algorithm produced a tag, so a mismatch looks
exactly like a wrong key: every datagram fails verification and is counted under `BAD_TAG`. Almost
nobody should set it.

#### Which key may speak for which application

One rule: **a name that any key claims is verified only by the keys claiming it; a name nothing
claims is verified by the keys that claim nothing.**

That a claim is a *reservation* and not merely a restriction is the whole of what makes it worth
having. Were a scoped key only limited to its names, a general key beside it would still verify
those names too, and a tenant with its own key would gain nothing — whoever held the general key
could still speak for it.

`applications` is a comma-separated list of patterns. `*` is the only metacharacter and matches any
run of characters, so `orders-*` covers a family without naming its members — which is what makes
scoping usable when the members are not known in advance. **These are not regular expressions**: as
one, `orders-*` would mean "orders followed by zero or more hyphens", matching `orders--` and not
`orders-prod`. A bare `*` means the same as leaving the setting out, so it cannot silently take
every unscoped key out of use.

Two keys covering one name — through overlapping patterns, or because nothing claims it — means
either key verifies it. That is not detected. Do not overlap.

The spectrum that produces, along one axis and independent of `config.source`:

| keyring | what you get |
|---|---|
| every key unscoped | any cluster may arrive, and **every keyholder can speak for every other** — one team, many clusters, one template |
| some scoped | a general key for the fleet, dedicated keys for the tenants that matter |
| every key scoped | only the named families can be served at all, and no keyholder can reach another's names |

The last row is a legitimate lockdown, and under `config.source=cluster` it is also how you get an
explicit tenant list while their timings still come from their own nodes — the scope lists *are*
the tenant list. It is indistinguishable from a witness that is quietly broken, so the startup log
says which it is:

```
witness 1023: 3 key(s), all scoped: only names matching orders-*, billing-*, *-test can be served
```

### Rotating a key

Rotation is not a mechanism here, it is list membership. No restart, no flag day:

1. Add the new key beside the old one. The witness now accepts either, and signs each reply with
   whichever key verified the request — so a node holding only one of them still hears an answer.
2. Roll the cluster's nodes onto the new key at your own pace.
3. Remove the old entry. It stops working.

Do not skip step 3. Two keys is a rotation window, not a permanent second key. If both entries
claim the same applications, note that during the window either verifies them, which is the point.

### Hot reload

The configuration directory is re-read while the server runs. **This is the point of the design**,
because restarting a witness is the one genuinely risky operation in its lifecycle: while it is
down, every two-node cluster it serves is back to tolerating zero failures.

- A whole-directory rescan on any file event, debounced ~250ms, so one save is one reload.
- A periodic rescan (~30s) as a genuine safety net — file watching is inotify on Linux but a slow
  polling fallback on macOS, and does not work at all on some network filesystems.
- **A malformed file never takes down the running configuration.** The candidate is parsed and
  validated whole, and published only if it is good; otherwise the last good configuration keeps
  serving and the failure is logged loudly.

Keys marked not hot-reloadable are read and ignored with a log line, rather than half-applied.

---

## Where configuration comes from — `config.source`

### `file` (default)

Serves exactly the applications configured in `applications/`, and drops everything else, counted
and logged once per unseen name. Nothing arriving over the network changes any configuration here.
This is the mode for a deployment whose operator has a hard constraint against automatic change.

### `cluster`

Serves any application name it hears, creating state for it. Less operational burden, a legitimate
choice — and the way a typo becomes a split brain.

**Only a node stating its own configuration creates an application.** A `Ping`, a `RequestVote` or a
heartbeat naming an application this witness does not serve is dropped and counted under
`awaitingNodeConfig`; nothing is created until one of that cluster's nodes says what intervals it is
running. That is ordinary and brief while a cluster starts — its first probes and votes arrive
before its first config message — and it is what stops the witness ever serving a tenant it has no
timings for. Sustained, it means a cluster is talking to this witness and never saying who it is.

**Its precondition, which is easy to miss:** a cluster-created application has no file, so a key in
the keyring has to cover it — either an unscoped one, or one whose pattern matches its name.
Nothing else is needed: its timings come from its own nodes, so a tenant on a different profile
needs no configuration here at all.

**Keys are the one thing that can never come from the cluster.** A key authenticates the datagram
that would have carried it, so taking one over the wire is circular; and the algorithm is
deliberately absent from the wire too, because a datagram naming its own algorithm would let an
attacker pick the weakest one both ends accept. That is why the keyring stays in
`witness.properties` under either mode — and why **how much isolation you get is decided by your
patterns, not by `config.source`**. See [Keys](#keys).

| differs | what happens |
|---|---|
| the key or its algorithm | every datagram fails verification, counted under `BAD_TAG`. Total, and obvious. Add a key covering that cluster |
| timings | nothing. The witness follows what that cluster's nodes state, per application, and rebuilds when they change |

**The walkthrough that is the reason `file` is the default:**

> Two clusters are deployed. One is configured `orders`. The other, through a typo in a config
> file, is configured `ordres`.
>
> The two sets of nodes drop each other's datagrams, because the application name does not match —
> the cross-talk protection working exactly as designed. So each side sees the other as
> unreachable, and each asks the witness for a vote.
>
> A witness on `config.source=cluster` auto-creates **two** state buckets, one per name, and grants a vote from
> each. Both sides now have a majority. **Two leaders, out of one typo** — and the very protection
> that should have caught the mismatch is what hides the cause, because neither cluster ever sees
> the other's traffic.

Permissive mode is hardened rather than merely documented:

- every auto-registration logged at WARN with the name and source address;
- **near-miss detection** on first sight of a name — case-insensitive, small edit distance against
  names already served — warning explicitly that it may be a typo. It also runs under `file`,
  where a dropped `ordres` next to a served `orders` is the entire diagnosis in one line;
- `config.cluster.limit`, so an open witness cannot be filled up by whoever can reach the port;
- state is **never** auto-expired, because deleting `votedFor` for a dormant cluster that later
  returns is precisely the double-vote bug.

---

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
| WARN | `no applications configured` | `config.source=file` with an empty `applications/` serves nobody |
| WARN | `no keys are configured` | see rule 3 |
| WARN | `... is world-readable and may contain a cluster secret` | fix the file mode |
| WARN | `dropping traffic for unserved application 'X'` | a cluster is asking and being refused |
| WARN | `'X' is one or two characters from 'Y'` | **near miss — check both configurations** |
| WARN | `auto-registering application 'X'` | `config.source=cluster` serving someone unconfigured |
| WARN | `refusing to auto-register` | `config.cluster.limit` reached |
| WARN | `stated intervals that do not hold together` | a cluster advertised a profile that could not be built; the last good one stays in force |
| INFO | `Holding off until one of its nodes states its configuration` | ordinary while a cluster starts; sustained means it never says who it is |
| ERROR | `cannot read the configuration in ...` | a bad edit; the last good config is still serving |
| ERROR | `application 'X' has failed` | that cluster is no longer served — see below |

### One application failing does not stop the server

If an application's state store fails — a full or broken disk — that application is **quarantined**:
it stops being served, and every other tenant keeps working. One team's storage problem must not be
every team's failover outage.

The clusters relying on that application's witness are back to the fault tolerance they have
without one. Fix the cause and restart the witness.

### State on disk

```
/var/lib/primos-witness/
├── orders/{state,state.lock}
└── billing/{state,state.lock}
```

A directory per application, so one cluster's state can be backed up, moved or removed on its own,
and two applications cannot share a file lock and thereby a failure. `state.lock` is an exclusive
lock — a second witness pointed at the same directory fails to start rather than quietly
double-voting.

Back it up if you like, but **never restore an old copy**: restoring a stale `votedFor` is
indistinguishable from forgetting a vote. If in doubt, an empty state directory for a *new* witness
identity is safer than a stale one for an existing identity.

### Removing an application

Delete `applications/<name>.properties`. It stops being served within one reload cycle. Its state
directory stays; remove it by hand only once you are certain the cluster is gone for good.

---

## Troubleshooting

**A cluster cannot reach the witness.** Its nodes log `the witness (N) is unreachable`. Check UDP
reachability on the port first, then that the witness logs the application as served, then that the
node ids match — the witness's `--node-id` must equal the id each cluster configures with
`Peer.witness(id, ...)`.

**The witness sees the traffic but never answers.** Almost always the secret. A datagram failing
authentication is dropped silently before anything is parsed, by design, so look at the drop
counters rather than for an error. Check that some key in the keyring covers that application —
a name claimed by a key the cluster does not hold is verified by nothing else — and check for a
rotation where the node holds only a key already removed from the ring.

**`dropping traffic for unserved application 'X'`.** Strict mode, and no `applications/X.properties`
exists. If `X` looks like a near miss of something you do serve, treat it as a typo in that
cluster's configuration and not as something to fix by adding a file.

**Two leaders in one cluster.** Check, in order: two witness instances answering for the same
identity (rule 1); a wiped or restored state directory (rule 2); two clusters whose names differ by
a typo on a witness with `config.source=cluster` (the walkthrough above).

**Failover is slower than expected.** The witness's intervals should match the deployment — `wan`
if it sits on another network, or `config.source=cluster` so the tenant's own nodes settle it. Both
sides log an ERROR naming the two interval sets when they disagree, so this is greppable rather than
guesswork. Note the witness does not campaign, so it never causes an election; it only answers.

**A cluster is talking to the witness but is never served.** Under `config.source=cluster` only a
node stating its configuration creates an application; look for `Holding off until one of its nodes
states its configuration` and check that cluster is running a version that sends it. Under
`config.source=file`, it needs `applications/<name>.properties`.

---

## What it does not do

No replication, no state machine, no snapshots, no runtime membership changes. The witness never
campaigns, never becomes leader, and never appears as one to any listener. It votes, and that is
all.

## License

Apache License 2.0 — see [LICENSE.txt](../LICENSE.txt).
