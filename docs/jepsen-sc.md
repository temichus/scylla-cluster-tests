# Running Jepsen locally (for the Strong Consistency work)

Plan: [docs/plans/testing/jepsen-strong-consistency.md](plans/testing/jepsen-strong-consistency.md).
This page is the "tier A" environment from that plan: Jepsen's own Docker setup, no SCT involved.
It is for developing and debugging the Clojure side (workloads, checkers, nemeses) on any machine
with Docker, including an Apple Silicon Mac.

What runs: one `control` container (Debian 12, JDK 17, Leiningen) and N `node` containers
(Debian 12 with systemd and sshd, Scylla `master:latest` pre-installed from the deb repo, developer
mode, `--memory 2G`). The control node runs `lein` from a bind-mounted checkout of
[scylladb/jepsen](https://github.com/scylladb/jepsen) and ssh-es into the nodes as root.
Jepsen 0.3.10 is the newest release whose jar runs on JDK 17; 0.3.11 and later ship classes built on
JDK 21, which Debian 12 does not package.

## One-time setup

```bash
git clone https://github.com/scylladb/jepsen ~/src/scylladb-jepsen
```

```bash
cd docker/jepsen-local && export JEPSEN_SCYLLA=~/src/scylladb-jepsen COMPOSE="-f fork.yml"
```

```bash
bash bin/up --init-only -n 3
```

`--init-only` generates `secret/` (an ed25519 key pair and the compose env files) and
`docker-compose.yml` for 3 nodes. Use `-n 5` for the 5-node layout the GCE job uses.

## Bring the cluster up

```bash
bash bin/up -d -n 3
```

First time this builds two images (node ≈ 1.5 GB, a few minutes). Then raise the host's AIO limit —
Scylla refuses to start otherwise; this is a VM-wide sysctl, so once per Docker Desktop restart:

```bash
docker exec jepsen-n1 sysctl -w fs.aio-max-nr=1048576
```

## Run a test

```bash
docker exec jepsen-control sh -c 'cd /jepsen/scylla && lein run test --workload none --nemesis none --no-install-scylla --time-limit 10 --concurrency 3 --node n1 --node n2 --node n3 --username root --ssh-private-key /root/.ssh/id_rsa'
```

`--no-install-scylla` selects `scylla.sc.db` (plan step 1.2): Jepsen starts, stops, kills and pauses
Scylla but never writes `/etc/scylla/scylla.yaml` or touches the data directory, exactly as on an
SCT-provisioned cluster. The nodes therefore configure themselves at boot
(`node/configure-scylla.sh`): addresses, seeds, `cluster_name: jepsen`, 2 shards, 2 GiB, and
`experimental_features: [strongly-consistent-tables]`. Jepsen stops Scylla at the end of a test
before collecting logs (pass `--leave-db-running` to keep it up); the next run starts it again
with its config and data intact.

Results: `store/latest/results.edn` in the fork checkout (`:valid? true` on success),
`store/latest/jepsen.log`, and per-node logs under `store/latest/n*/`. The first `lein run`
downloads the dependency tree (~1 min); later runs take ~30 s for `--workload none`.

## Reading `results.edn`

One file per test under `store/<test name>/<timestamp>/`; `store/latest` points at the last run.
The top-level `:valid?` is the verdict: `true`, `false`, or `:unknown` when the checker gave up
(Knossos search timeout). The map has one entry per checker:

| Key | What it says |
| --- | --- |
| `:workload` | the consistency checker. Elle lists `:anomaly-types` (`:G-single-item-realtime`, `:cyclic-versions`, …) and the cycles under `:anomalies`; Knossos (`jepsen.independent`) nests per-key `:results {<key> {:linearizable {:valid? false :op … :previous-ok …}}}` with the op that cannot be explained. |
| `:stats` | `:ok-count`, `:fail-count`, `:info-count` overall and `:by-f`. On SC, writes must be `:ok` or `:info` (indeterminate), never `:fail` once sent; a green run with every write `:info` proves nothing. |
| `:exceptions` | `:valid? false` on any driver exception the client did not map; the run is then a harness failure, not a verdict. |
| `:perf`, `:clock` | graphs only. |

SCT reads the same file: `sdcm.utils.jepsen.JepsenTestResult` (an EDN reader is in the same module),
`jepsen_test.py` turns each test into a `JepsenEvent` (CRITICAL on `false`, WARNING on `:unknown`,
ERROR when no `results.edn` was written) and one Argus row per (workload, nemesis). The verdict is
never the exit code of `lein`.

## Adding a workload

Copy `src/scylla/sc/register.clj` (Knossos, one row per key) or `src/scylla/sc/wr_register.clj`
(Elle, single-partition batches), register it in `scylla.core/workloads`, and keep the client rules
of plan §2.6: every statement goes through `scylla.sc.client/execute`, `with-errors` wraps
`invoke!`, `retry-each` only around the `CREATE TABLE` in `setup!`, no `IF`, no `USING TIMESTAMP`,
no counters, lists or multi-partition batches (SC rejects them with `InvalidQueryException`). Tables
live in keyspace `scylla.sc.db/keyspace` (`jepsen_sc`), which `scylla.sc.db` drops and recreates
per run. Then the two runs every workload must pass: SC `:valid? true`, and the EC canary
(`--no-sc --nemesis partition --read-consistency one --write-consistency one`) `:valid? false`.

Interactive shell on the control node: `bash bin/console`. Results browser:
`docker exec -d jepsen-control sh -c 'cd /jepsen/scylla && lein run serve'` then
`docker port jepsen-control 8080`.

## Tear down

```bash
docker compose -p jepsen -f docker-compose.yml -f fork.yml down -v
```

## What was patched, and why

The directory is `jepsen-io/jepsen` at commit `50de29bb` (`docker/`, removed upstream on
2026-03-29) plus:

| Patch | Where | Why |
| --- | --- | --- |
| JDK from apt (`openjdk-17-jdk-headless`) instead of an x86-64 Temurin tarball | `control/Dockerfile` | arm64 hosts |
| No copy of the parent directory into the control image; fork is bind-mounted via `fork.yml` | `bin/up`, `control/Dockerfile` | the upstream layout expects the Jepsen monorepo as parent |
| Scylla master pre-installed, `developer_mode`, `MEM_CONF=--memory 2G`, service disabled | `node/Dockerfile` | `--no-install-scylla`; several nodes on one host |
| `scylla.yaml` written at boot by `04-configure-scylla` (addresses, seeds, SC flag, `--smp 2`) | `node/configure-scylla.sh` | with `scylla.sc.db` Jepsen no longer configures the nodes; SCT does this for its nodes |
| `/usr/sbin/tcpdump -> /usr/bin/tcpdump` | `node/Dockerfile` | Jepsen hard-codes `/usr/sbin`; same symlink as `jepsen_test.py:58` |
| ed25519 key instead of RSA | `bin/up` | jsch signs RSA with SHA-1, Debian 12 sshd rejects it; SCT's node keys are ed25519 too |

Both apt signing keys (2024 `A43E06657BAC99E3`, 2026 `C503C686B007F39E`) are imported; the master
repo is signed with the 2026 key. The key list is the same as `defaults/test_default.yaml`.
