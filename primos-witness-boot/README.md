# primos-witness-boot

The witness server as a Spring Boot application: actuator health, configuration from the
environment, and the packaging [the container image](../PLAN.md) is built from. A front door onto
[`primos-witness`](../primos-witness); the other one is the tarball in
[`primos-witness-cli`](../primos-witness-cli).

**The operator manual is [`primos-witness/README.md`](../primos-witness/README.md).** Almost
everything worth knowing — the three safety rules, the keyring, `config.source`, hot reload, what to
alert on — describes server behaviour every packaging shares, so it lives with the server. This file
covers only what is different here.

## Which packaging

| | `primos-witness-cli` | this module |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image, or the jar |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log | `/actuator/health`, and `/actuator/metrics` per served cluster |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application, with Tomcat |
| **startup, measured** | **~0.44s** | **~4.0s** |

The startup row is not a detail: **the window in which a witness is starting is the window in which
every two-node cluster it serves tolerates zero failures.** Both numbers are small and the ratio is
about nine to one; which matters depends on how often you restart a witness, and the answer should
be "almost never". Choose this packaging because you already run containers and want the health
endpoint and environment configuration; choose the tarball because you want a process with no
framework in it that can sit untouched for years.

Neither front door holds any logic of its own. Both build a `WitnessServer` and run it; if either
grows behaviour, or anything is copied between them, that is the thing to delete.

## Quick start

```bash
java -jar primos-witness-boot-<version>.jar \
  --primos.witness.config-directory=/etc/primos-witness
```

The directory is the one the manual describes: `witness.properties` beside an `applications/`
directory with a file per served cluster. Everything in it is re-read while the server runs, so
onboarding a cluster never needs a restart.

Spring's shutdown stops the server: each served cluster is sent its `Leaving` datagram and every
state store is closed before the JVM exits. Give the process time to do it —
`terminationGracePeriodSeconds` or `--stop-timeout` must exceed
`spring.lifecycle.timeout-per-shutdown-phase`, which defaults to 30s and bounds the wait.

## Configuration

**Spring binds three settings, and the rest stay in the file.** These three are the tarball's three
command-line options, under the names `witness.properties` already uses:

| property | environment variable | what it is |
|---|---|---|
| `primos.witness.config-directory` | `PRIMOS_WITNESS_CONFIGDIRECTORY` | `--config`; default `/etc/primos-witness` |
| `primos.witness.node-id` | `PRIMOS_WITNESS_NODEID` | `--node-id`; default `1023` |
| `primos.witness.bind.host` / `.port` | `PRIMOS_WITNESS_BIND_HOST` / `_PORT` | `--bind`; either half may be given alone, and the other comes from the file |

**Everything else — the state directory, `config.source`, the keyring, the timings, vote logging,
the rescan interval — comes from `witness.properties`, and that is a rule rather than an
omission.** Spring binds once, at boot. A key expressed in both places would freeze at whatever
Spring read while an operator edited the file and watched nothing happen — and the file is re-read
on purpose, because restarting a witness is the one genuinely risky operation in its lifecycle. The
keyring settles it on its own: a secret bound at boot cannot be rotated without a restart.

Setting one of those here anyway is refused at startup, with a message naming both places. It is not
ignored, because ignored is how the state directory ends up somewhere nobody expects and is
discovered when a restart cannot find a vote.

### The properties store, when there is no file at all

`primos.witness.config-store=properties` moves the whole configuration into Spring properties.
**It costs hot reload**, which is the reason the file store is the default: adding a cluster or
rotating a secret then means restarting the witness. It is a fair trade for a single-tenant
deployment whose served set genuinely never changes, and a bad one for anything else. Choosing it
logs a warning saying so.

```properties
primos.witness.config-store=properties
primos.witness.state-directory=/var/lib/primos-witness
primos.witness.bind.port=7420

primos.witness.applications[0].name=orders
primos.witness.applications[1].name=billing
# Per application, because a witness serves clusters it does not own and one of them on another
# network must not set the intervals for all of them.
primos.witness.applications[1].timings.profile=wan

# The default for any application that names none of its own, and what an application
# auto-registered under config.source=cluster starts on.
primos.witness.timings.profile=lan

primos.witness.keys.platform.value=${PLATFORM_WITNESS_SECRET}
primos.witness.keys.orders.value=${ORDERS_WITNESS_SECRET}
primos.witness.keys.orders.applications=orders-*,*-test
```

| property | what it is |
|---|---|
| `state-directory` | required; must be durable storage that survives a restart |
| `config-source` | `file` (serve only what is listed) or `cluster` (a cluster may create itself) |
| `cluster-application-limit` | how many `cluster` may create; default 64 |
| `applications[n].name` | a served cluster |
| `applications[n].timings.*` | its intervals, or the witness default below |
| `timings.profile` | `lan` or `wan` |
| `timings.heartbeat-interval`, `.election-timeout-min`, `.election-timeout-max`, `.probe-interval`, `.advertisement-interval`, `.failure-threshold` | the intervals spelled out — a profile **or** these, never both |
| `vote-logging` | who voted for whom, at INFO; default true |
| `keys.<name>.value` | the shared secret |
| `keys.<name>.algorithm` | the JCE `Mac`; default HMAC-SHA256 |
| `keys.<name>.applications` | the name patterns this key may speak for; `*` matches any run of characters |

The keyring rule is the manual's and is not relaxed here: **a name that any key claims is verified
only by the keys claiming it**, and a name nothing claims is verified by the keys that claim
nothing. Never run without a key.

Two spellings worth knowing, because nobody guesses them: a list index becomes
`PRIMOS_WITNESS_APPLICATIONS_0_NAME`, and durations are Spring's, so `250ms` and `2s` rather than
the file's `.millis` integers.

## Health

`/actuator/health` is `UP` exactly while this witness is serving, and its detail names what it
serves and what it has stopped serving:

```json
{"status":"UP","components":{"witness":{"status":"UP","details":{
  "boundAddress":"/0.0.0.0:7420","served":["orders","billing"],"quarantined":[]}}}}
```

**One quarantined tenant does not make the witness `DOWN`.** A witness serving fifty clusters with
one quarantined is doing its job for forty-nine, and a verdict that says otherwise takes all fifty
out of rotation over one of them. A quarantined application — most often one whose state can no
longer be made durable — belongs in the detail, where somebody can act on it. `DOWN` means one
thing: the loop is not running, so this process is answering HTTP and serving nobody.

**Do not point a liveness probe at it.** Restarting a witness is the one genuinely risky operation
in its lifecycle, so a probe that can be wrong is a mechanism for causing the outage it is watching
for. Spring Boot ships `/actuator/health/liveness` by default and **this module switches the probe
groups off** for exactly that reason; a readiness check needs no group and can read
`/actuator/health` directly. Boot's disk-space contributor is off too, and for the same reason: it
watches a directory this process does not care about, and nothing but the witness may answer whether
the witness is serving.

## Metrics

`primos-micrometer` is on the classpath, so `/actuator/metrics` carries the witness's own routing
counters and a full set of node meters **per served cluster**, tagged with that cluster's name and
this witness's node id. Nothing to configure; it binds when the application has a `MeterRegistry`.

The two worth alerting on:

- `primos.witness.routing{reason="undeliverable_replies"}` should stay at zero. A witness that
  silently fails to answer looks exactly like a witness nobody is asking, while the cluster on the
  other end waits out an election timeout for a vote that was never coming.
- `primos.witness.applications{state="quarantined"}` above zero is the health detail as a number.

The catalogue, and the tag rules, are in [`primos-micrometer`](../primos-micrometer/README.md).

## Startup

Measured on the packaged jar, from exec to serving, five runs: **~0.44s** for the tarball and
**~4.0s** here. `spring.main.lazy-initialization=true` brings this to ~3.6s and is **not** enabled:
0.4s does not buy moving a misconfiguration from startup to first use, in a server whose whole
posture is to fail loudly before it is trusted with a vote.

Adding the metrics was measured rather than assumed, since this is the number that matters most
here: binding them costs **~0.2s**, which is inside the run-to-run spread of the measurement.

## Build

```bash
./mvnw -o -pl primos,primos-witness,primos-witness-boot package -DskipTests
```

leaves a layered, runnable `target/primos-witness-boot-<version>.jar`. Nothing in the build talks to
a Docker daemon: `./mvnw verify` stays green on a machine that has none.
