# Running Jepsen locally (for the Strong Consistency work)

Plan: [docs/plans/testing/jepsen-strong-consistency.md](plans/testing/jepsen-strong-consistency.md).
This page is the "tier A" environment from that plan: Jepsen's own Docker setup, no SCT involved.
It is for developing and debugging the Clojure side (workloads, checkers, nemeses) on any machine
with Docker, including an Apple Silicon Mac.

What runs: one `control` container (Debian 12, JDK 17, Leiningen) and N `node` containers
(Debian 12 with systemd and sshd, Scylla `master:latest` pre-installed from the deb repo, developer
mode, `--memory 2G`). The control node runs `lein` from a bind-mounted checkout of
[scylladb/jepsen](https://github.com/scylladb/jepsen) and ssh-es into the nodes as root.

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

`--no-install-scylla` skips `apt-get install` on the nodes (Scylla is in the image). Note that
Jepsen 0.2.4 still rewrites `/etc/scylla/scylla.yaml` and wipes the data directory on every run
(plan §2.2) — so until plan step 1.2 lands, the cluster runs with Jepsen's template config, which
has no `experimental_features`, and SC DDL is rejected.

Results: `store/latest/results.edn` in the fork checkout (`:valid? true` on success),
`store/latest/jepsen.log`, and per-node logs under `store/latest/n*/`. The first `lein run`
downloads the dependency tree (~1 min); later runs take ~30 s for `--workload none`.

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
| Dummy `netcat` 1.10-99 package (equivs) | `node/Dockerfile` | Jepsen 0.2.4 runs `apt-get install netcat`; no candidate on Debian 12 |
| `/usr/sbin/tcpdump -> /usr/bin/tcpdump` | `node/Dockerfile` | Jepsen hard-codes `/usr/sbin`; same symlink as `jepsen_test.py:58` |
| ed25519 key instead of RSA | `bin/up` | jsch signs RSA with SHA-1, Debian 12 sshd rejects it; SCT's node keys are ed25519 too |

Both apt signing keys (2024 `A43E06657BAC99E3`, 2026 `C503C686B007F39E`) are imported; the master
repo is signed with the 2026 key. The key list is the same as `defaults/test_default.yaml`.
