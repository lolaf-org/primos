# Witness from the tarball

`primos-witness-cli` packages the witness as a process you install: a small command line and a `tar.gz` an operator
unpacks beside a systemd unit. Everything about the witness itself (the safety rules, the keyring, `config.source`, hot
reload, what to alert on) is in the [witness manual](witness.md), shipped in the tarball as `MANUAL.md`. This page
covers what is particular to the tarball: unpacking it, the command line, and the unit that supervises it.

## Which packaging

| | the tarball | [Spring Boot and containers](witness-spring-boot.md) |
|---|---|---|
| deployment | a tarball and a systemd unit | a container image, or the jar |
| configuration | `witness.properties` and `applications/` | the same, plus Spring's environment |
| health, metrics | the log | `/actuator/health`, and `/actuator/metrics` per served cluster |
| dependencies in the process | picocli and slf4j-simple | a Spring Boot application, with Tomcat |
| **startup, measured** | **~0.44s** | **~4.0s** |

The startup row is not a detail: **the window in which a witness is starting is the window in which every two-node
cluster it serves tolerates zero failures.** Both numbers are small and the ratio is about nine to one. Choose the
tarball for a process with no framework in it that holds every served cluster's secret and can sit untouched for years;
choose the image if you already run containers and want the health endpoint and environment configuration.

Both build the same witness from the same configuration directory, and neither holds any logic of its own.

## Installing

The tarball unpacks into a `primos-witness/` directory and runs from there: its configuration in `conf/`, its state in
`data/`, both beside the launcher.

Download it from the [GitHub Release](https://github.com/lolaf-org/primos/releases):

```sh
cd /opt
curl -fLO https://github.com/lolaf-org/primos/releases/download/v0.9.0/primos-witness-cli-0.9.0.tar.gz
tar xzf primos-witness-cli-0.9.0.tar.gz
cp -r primos-witness/conf-sample primos-witness/conf
chmod 0750 primos-witness/conf
```

```
/opt/primos-witness/
├── bin/primos-witness
├── lib/*.jar
├── conf-sample/                 # heavily commented; copy it to conf/, never edit it
│   ├── witness.properties
│   └── applications/example.properties
├── conf/                        # yours: witness.properties, applications/, keys
├── data/                        # the term and vote of every served cluster, created on first start
├── MANUAL.md                    # the witness manual
├── README.md                    # this page
└── LICENSE.txt
```

`data/` comes from `state.directory=../data` in the sample: a relative path is resolved against the configuration
directory, so it does not depend on where the process is started from. **It must survive**: a witness that forgets a
vote can grant a second one in the same term and elect two leaders. Put `/opt/primos-witness` on durable local
storage, or set `state.directory` to an absolute path that is.

Requires a JRE 11 or newer on `PATH`, or `JAVA_HOME` set.

## Upgrading

Unpack the new tarball over the same directory, after removing `lib/`:

```sh
cd /opt
rm -rf primos-witness/lib
tar xzf primos-witness-cli-<new-version>.tar.gz
```

`conf/` and `data/` are not in the tarball, so unpacking cannot replace your keys or forget a vote; only
`conf-sample/`, `bin/`, `lib/` and the documents are replaced. `lib/` is removed first because the launcher loads every
jar in it, and the old version's jars would otherwise sit beside the new ones.

## Running

```sh
bin/primos-witness
```

Without `--config` it reads `conf/` beside `bin/`, and refuses to start if there is none.

| option | default | notes |
|---|---|---|
| `--config <directory>` | `conf/` in the unpacked directory | must exist |
| `--node-id <id>` | `1023` | 0 to 1023. **Fixed once deployed**; the [manual](witness.md#the-node-id-is-durable) says why |
| `--bind <host:port>` | from configuration | overrides `bind.host`/`bind.port` |
| `-h`, `--help` | | |
| `-V`, `--version` | | which build this box is running |

Three options and no more, deliberately. Everything else (the state directory, `config.source`, the keyring, the
timings) comes from `witness.properties`, which is re-read while the server runs. A setting that could also be given
here would be frozen at the value this process started with, and restarting a witness is the one genuinely risky
operation in its lifecycle.

Exit codes, for whatever supervises it: **0** normal, **2** for a mistyped option or a missing configuration directory
(reported as one sentence and the usage text), and **1** for a failure to start, which is logged with its cause.

## systemd

```ini
[Unit]
Description=Primos witness server
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=primos
Group=primos
ExecStart=/opt/primos-witness/bin/primos-witness
Restart=on-failure
RestartSec=5s

# The state directory must survive. Everything else may be locked down.
ReadWritePaths=/opt/primos-witness/data
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

`SIGTERM` is a clean shutdown: the loop finishes its turn, tells every served cluster it is going away, and closes its
state stores before exiting. systemd's `TimeoutStopSec` defaults to 90s, which is ample.

## Building it

```bash
./mvnw -o -pl primos,primos-witness,primos-witness-cli package -DskipTests
```

leaves `primos-witness-cli/target/primos-witness-cli-<version>.tar.gz`, laid out as *Installing* above shows.
