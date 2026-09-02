# primos-witness-cli

The witness server as a process you install: a picocli command line over
[`primos-witness`](../primos-witness), and a `tar.gz` an operator can unpack beside a systemd unit.

**The operator manual is [`primos-witness/README.md`](../primos-witness/README.md)**, and it ships
inside the distribution so it is to hand on a box with no browser. Almost everything worth knowing —
the three safety rules, the keyring, `config.source`, hot reload, what to alert on — describes
server behaviour that every packaging shares, so it lives with the server rather than with a front
door onto it.

## Which packaging

| | this module | `primos-witness-boot` *(planned)* |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log, and a `PrimosStatsListener` if you embed it | actuator endpoints |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application |
| startup | a fraction of a second | a second or two |

That last row is not a detail: **the window in which a witness is starting is the window in which
every two-node cluster it serves tolerates zero failures.** It is the strongest argument for keeping
this packaging alongside the containerised one, together with "no framework anywhere near a process
that holds every served cluster's secret and is meant not to move for years".

Neither front door holds any logic of its own. Both build a `WitnessServer` and run it; if either
grows behaviour, or anything is copied between them, that is the thing to delete.

## Build

```bash
./mvnw -o -pl primos,primos-witness,primos-witness-cli package -DskipTests
```

leaves `target/primos-witness-cli-<version>-dist.tar.gz` — `bin/`, `lib/`, `conf-sample/`,
`LICENSE.txt` and the manual. `conf-sample/` is deliberately not installed over a live `conf/`: an
upgrade that replaced `witness.properties` would replace the secrets with placeholders.
