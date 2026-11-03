---
status: draft
domain: testing
created: 2026-11-03
last_updated: 2026-11-03
owner: null
---

# Jepsen for Strong Consistency tables — revive the harness, rewrite the workloads

Ticket: [SCYLLADB-42](https://scylladb.atlassian.net/browse/SCYLLADB-42) (P0, `status/release_blocker` since 2026-11-03).
Related: [Porcupine research](https://scylladb.atlassian.net/wiki/spaces/RND/pages/470155330) (SCYLLADB-4347),
[sc-verify decision](https://scylladb.atlassian.net/wiki/spaces/RND/pages/476446732) (SCYLLADB-4519),
[SC testing summary](https://scylladb.atlassian.net/wiki/spaces/RND/pages/503611736).

All work in this plan is done by AI agents. Every step therefore has a command an agent can run
and a definition of done an agent can check without a human. Humans decide only the items in §7.

## 1. Problem Statement

Strong Consistency (SC) tables promise linearizability. Today that promise is checked by:

- `test.py` + Porcupine ([scylladb PR 29085](https://github.com/scylladb/scylladb/pull/29085)): nightly,
  small cluster, schema changes in the background. Found SCYLLADB-1563.
- sc-verify (cql-stress + Porcupine, SCYLLADB-4519): real clusters, hours, under nemesis. Validated
  locally; the SCT job is not wired yet.

The Jepsen job in SCT exists but tests none of this: every workload it runs uses LWT, counters or
collections, which SC rejects, and Jepsen rewrites `scylla.yaml` on the nodes so the SC feature flag
is lost before the first query. The job is also a weak gate (passes on 1 green run of 5) and reports
nothing to Argus.

SCYLLADB-42 was raised to P0 today with the request to "have another attempt to review the Jepsen
tool" (Alex Dathskovsky, Jira comment 2026-11-03). Petr Gusev and Gleb Natapov expect the value to
be Jepsen's fault injection (network, clocks, membership) and its checkers, not a second
Porcupine. This plan says what to keep, what to replace, and in which order.

## 2. Current State

### 2.1 What the SCT job does today

| Piece | Where | What it does |
| --- | --- | --- |
| Pipeline | [vars/jepsenPipeline.groovy](../../../vars/jepsenPipeline.groovy) | Standard SCT pipeline, GCE only (`--backend gce` hard-coded at `:233,235`). Creates and finishes an Argus run (`:131-145`, `:305-320`) but nothing in the test publishes results to it. |
| Jobs | [jenkins-pipelines/oss/jepsen/jepsen-all.jenkinsfile](../../../jenkins-pipelines/oss/jepsen/jepsen-all.jenkinsfile), [jenkins-pipelines/oss/raft/jepsen-with-raft.jenkinsfile](../../../jenkins-pipelines/oss/raft/jepsen-with-raft.jenkinsfile) | Two jobs, same pipeline, two YAMLs. |
| Test cases | [test-cases/jepsen/jepsen.yaml](../../../test-cases/jepsen/jepsen.yaml), [jepsen_with_raft.yaml](../../../test-cases/jepsen/jepsen_with_raft.yaml) | 5 DB nodes `n2-standard-4`, 1 loader, Debian 12, Scylla installed from repo (`use_preinstalled_scylla: false`), `NoOpMonkey`, `bare_loaders: true` (`:20`), `jepsen_test_count: 5` + `jepsen_test_run_policy: any` (`:18-19`). The raft variant only adds `experimental_features: [raft]` (`:22-23`), which has been the default since 2023. |
| Tester | [jepsen_test.py](../../../jepsen_test.py) | `setup_jepsen()` (`:35-60`) installs `openjdk-11-jre` by apt on the loader, downloads `lein`, clones the Jepsen repo, copies the DB SSH key. `test_jepsen()` (`:80-91`) runs each `jepsen_test_cmd` as `lein run <cmd> --node … --no-install-scylla` and asserts the policy result. |
| Policy | [jepsen_test.py:66-78](../../../jepsen_test.py) | `any` = one `.ok` exit of N runs passes. With the YAML above: **1 green of 5**. |
| Defaults | [defaults/test_default.yaml:180-191](../../../defaults/test_default.yaml) | Default repo `jepsen-io/scylla` (last commit 2020-12-11, dead). 7 commands: `test-all -w {cas-register,counter,cmap,cset,write-isolation,list-append,wr-register} --concurrency 10n`. The test case overrides the repo to `scylladb/jepsen` (last commit 2024-07-02). |
| Config | [sdcm/sct_config/mixins/jepsen.py](../../../sdcm/sct_config/mixins/jepsen.py) | 4 options: repo, cmd, count, policy. |
| Report | [jepsen_test.py:93-110](../../../jepsen_test.py) | Starts `lein run serve` on the loader and saves its index page as `jepsen_report.html`. That page is a list of runs, not the results. |
| Log collector | [sdcm/logcollector.py:1738-1753](../../../sdcm/logcollector.py) | Archives `~/jepsen-scylla` on the loader as `jepsen-data`, uploads to S3. `archive_log_remotely()` (`:914`) now produces `.tar.zst`. |
| Results viewer | [sdcm/utils/jepsen.py:79-86](../../../sdcm/utils/jepsen.py), [sct.py:2425-2442](../../../sct.py) | `hydra investigate show-jepsen-results <test_id>` downloads the archive as `jepsen_data.tar.gz` and runs `tar xzf`. Fails on the `.tar.zst` the collector produces. Uses image `scylladb/hydra-loaders:jepsen-20240729` ([utils/jepsen.py:23](../../../sdcm/utils/jepsen.py)). |
| Docker image | [docker/jepsen/](../../../docker/jepsen/) | A copy of jepsen-io's docker control node (JDK 21, lein). Used only by the results viewer. |
| Metadata | both YAMLs | No `test_metadata`, although `jepsen` is already a valid `test_type` ([unit_tests/unit/test_test_metadata.py:107](../../../unit_tests/unit/test_test_metadata.py)). |

Which Scylla version the jobs run today, and when they last passed, is **Needs Investigation**
(Jenkins/Argus history; not reachable from this repo). The job takes `scylla_version` as a parameter
with no default (`jepsenPipeline.groovy:32-34`).

### 2.2 What the upstream Jepsen code does

Repo: [scylladb/jepsen](https://github.com/scylladb/jepsen), a fork of
[jepsen-io/scylla](https://github.com/jepsen-io/scylla) by Kyle Kingsbury (2020). 3,200 lines of
Clojure. `project.clj`: Jepsen 0.2.4 (2021; current is 0.3.x), Clojure 1.10, Java driver
`scylla-driver-core 3.7.1-scylla-2` (no tablet-aware routing; SC writes take a forwarding hop,
which costs checker time, not soundness).

`src/scylla/core.clj`:
- `standard-workloads` (`:57-65`): cas-register, counter, cmap, cset, write-isolation,
  list-append, wr-register. `mv` excluded because it is expected to fail.
- `standard-nemeses` (`:72-79`): `[]`, `[:pause]`, `[:kill]`, `[:partition]`, `[:membership]`,
  `[:pause :kill :partition :clock :membership]`. `:membership` = decommission, remove, repair via
  nodetool (`src/scylla/nemesis.clj`).
- `all-tests` (`:311-319`): `test-all -w X` = 6 nemesis sets × 1 workload × `--test-count`, each with
  the Jepsen default `--time-limit 60` s. So one SCT command is six 60-second tests; the 7 commands in
  the defaults are 42 tests, and `jepsen_test_count: 5` repeats that 5 times.

`src/scylla/db.clj` — this is what breaks the SCT integration:
- `setup!` (`:457-461`) runs `configure!` on every node even with `--no-install-scylla`.
  `configure-scylla-yaml!` (`:379-391`) overwrites `/etc/scylla/scylla.yaml` with Jepsen's own
  `resources/scylla.yaml` (SimpleSnitch, `num_tokens: 256`, `developer_mode: true`, no
  `experimental_features`). SCT's configuration, including `experimental_features:
  [strongly-consistent-tables]` and tablets settings, is gone.
- `wipe!` (`:407-419`) kills Scylla and deletes `/var/lib/scylla/{data,commitlog,hints}` on setup
  and teardown. `--no-install-scylla` only skips `apt-get install`; the cluster is re-formed by
  Jepsen each test.

Workloads versus SC restrictions (SC rejects LWT, counters, MV/SI, CDC, client timestamps,
multi-partition batches; tested by 29 cqlpy tests, see testing summary):

| Workload | File | Why it cannot run on SC |
| --- | --- | --- |
| cas-register | `cas_register.clj:53,62,81` | `IF value = ?`, `IF value IN (…)`, `:serial` reads. Pure LWT. |
| list-append | `list_append.clj:37,107,90` | `IF lwt_trivial = null` on every write (a trick to force Paxos), `:serial` reads. |
| wr-register | `wr_register.clj:58,100,82,112` | Same `lwt_trivial` trick, `:serial` reads. Checker expects `:strict-serializable` only with `--lwt`. |
| counter | `counter.clj` | Counters not supported on SC. |
| cmap, cset | `collections/*.clj` | Collections on SC tables: untested upstream (Porcupine research §6 V6). |
| write-isolation | `write_isolation.clj` | Single-partition BATCH without IF. **Possibly runnable** — Needs Investigation (step 2.4). |
| mv | `mv.clj` | MV not supported on SC. |

Checkers in use: Knossos `linearizable` with the `cas-register` model (cas-register workload);
Elle `rw-register` (wr-register); Elle `list-append` (list-append). Elle ships in Jepsen 0.2.4.

### 2.3 Why the job is flaky or broken (summary)

1. **No SC coverage at all**: every standard workload needs LWT or a feature SC rejects (§2.2 table).
2. **`scylla.yaml` overwritten and data wiped** by `db.clj` — SC cannot even be enabled from SCT.
   Verified locally: after a Jepsen run the nodes have `cluster_name: 'jepsen'`, `num_tokens: 256`,
   `SimpleSnitch`, `experimental_features = []`, and
   `CREATE KEYSPACE … consistency = 'global'` fails with `ConfigurationException: The
   strongly_consistent_tables feature must be enabled`.
3. **Gate passes on 1 green of 5** (`jepsen.yaml:18-19` + `run_jepsen_cmd_any`).
4. **Nothing reaches Argus**: no results table, no events; the Jepsen `results.edn` is never read.
   Pass/fail is one `assertTrue` on process exit codes (`jepsen_test.py:91`).
5. **`bare_loaders: true`** and `NoOpMonkey`: no SCT stress, no SCT nemesis, no throughput data.
6. **Results viewer broken**: `.tar.zst` written, `.tar.gz` expected.
7. **Loader bootstrap likely fails on Debian 12**: `apt-get install openjdk-11-jre` (`jepsen_test.py:38`)
   — bookworm ships OpenJDK 17, not 11. Needs Verification (step 0.2).
8. **Stale pins**: Jepsen 0.2.4, dead default repo, `experimental_features: raft`.
9. **No `test_metadata`**, so the job is invisible to the pipeline catalogue.
10. **Jepsen 0.2.4 cannot set up a Debian 12 node.** `jepsen.os.debian/setup!` runs
    `apt-get install netcat libzip4`; on bookworm `netcat` is a virtual package with no candidate
    (`E: Package 'netcat' has no installation candidate`, exit 100) and Jepsen crashes before it
    touches Scylla. Both test cases use `debian-12` DB images (`jepsen.yaml:4`). Reproduced locally
    (2026-11-03, §5 tier A); fixed upstream in Jepsen 0.3.x (`netcat-openbsd`). Until the upgrade
    (5.2) the fix is a dummy `netcat` package on the nodes, or installing it from the SCT side.
11. **`/usr/sbin/tcpdump` is hard-coded** in Jepsen's tcpdump helper; Debian 12 has `/usr/bin`.
    SCT already works around it (`jepsen_test.py:56-58`). Reproduced locally.
12. **SSH key type matters.** The bundled jsch signs RSA keys with SHA-1, which Debian 12 sshd
    rejects (`Failed publickey … RSA`, then password fallback). SCT's node keys are ed25519
    (`sdcm/keystore.py:291`), so the GCE job is fine; a local setup must not use RSA.

Verified locally (2026-11-03, 3 nodes, Scylla `2027.1.0~dev-0.20261102`): with 10–12 worked
around, `lein run test --workload none --nemesis none --no-install-scylla` passes, so the
`scylla.yaml` template itself still boots current master (3 nodes up in ~20 s). It just boots it
without SC.

### 2.4 What sc-verify already covers (do not duplicate)

From the decision page and local validation report: per-row linearizability with Porcupine, live
invariants on every read, canaries, end-of-run durability read-back; validated through restarts,
`kill -9`, pauses, partitions and quorum loss; planned SCT job with topology nemesis
(restart, kill, decommission, network block). Elle is kept only as an offline second opinion and was
never run. **No SCT nemesis manipulates clocks** (research §10.6, verified).

### 2.5 Why the job retries until it passes, and how to remove that

The retry was added in [SCT PR 3352](https://github.com/scylladb/scylla-cluster-tests/pull/3352)
(2021-04-05, commit `5c04bfda8`): "Another attempt to stabilize Jepsen tests without modification
of the Clojure code … 5 runs with `any` policy for our regular Jepsen job." The PR links
[scylladb/jepsen#12](https://github.com/scylladb/jepsen/pull/12) "increase wait time for DB up to
20×6 s" as the other half of the fix. Later stability fixes in the fork are of the same kind:
[#15](https://github.com/scylladb/jepsen/pull/15) "Bootstrap nodes sequentially" (concurrent
bootstrap became unsafe in Scylla) and [#17](https://github.com/scylladb/jepsen/pull/17)
"set `force_schema_commit_log: true`" ("jepsen test isn't taking the default scylla.yaml, so some
of the defaults are not available").

So the flakiness that the retry hides is **not** checker noise. It is Jepsen re-forming the cluster
before every test with its own `scylla.yaml` and a wiped data directory (`db.clj` `configure!` /
`wipe!`, §2.2): bootstrap timing, seed order, and every Scylla config default the old template
lacks. Each new Scylla release that changes a boot requirement breaks the template again (#17 is
exactly that), and the retry turns the breakage into "sometimes green".

What follows from this:
- Step 1.2 (stop `configure!`/`wipe!` under `--no-install-scylla`) removes the cause: SCT provisions
  and configures the cluster once, with the real defaults, and Jepsen only stops/starts/kills
  processes for its nemeses.
- Step 3.2 (read `results.edn`) separates the three outcomes the exit code conflates today:
  consistency violation, checker `:unknown`, and harness failure (ssh, lein deps, node not up).
  Each gets its own event; none is retried.
- Step 1.5 then sets `jepsen_test_count: 1`, policy `all`. The `most`/`any` policies stay in the
  mixin only until the old jobs are retired (5.3); the new test case never uses them.
- Done when: 5 consecutive runs of the new job (5.1) have zero harness failures without any retry.
  If a harness failure still appears, it is fixed at its cause (a timeout in `db.clj`, a missing
  package), never by re-running.

### 2.6 The other retries: four layers, one rule each

"Retry" means four different things in this stack. The SCT one (§2.5) is the visible one; the
dangerous one is the driver.

| Layer | Where today | What it does | Rule for `scylla.sc.*` |
| --- | --- | --- | --- |
| **1. Java driver** | `client.clj:130-132`: `--retry` option, default `:default` = the driver's `DefaultRetryPolicy`; `never-retry` exists (`:84-100`) but the SCT job never passes `--retry never` | Re-issues a statement *below* the history on unavailable / read-timeout (and write-timeout for batch-log). A re-issued write is a second write the history does not know about — the checker then sees one op where the DB saw two. | **`never-retry`, hard-wired**, not an option. Also set `setIdempotent(false)` on every statement and leave speculative execution off (both are driver defaults in 3.x; assert them in a unit test so an upgrade cannot flip them). |
| **2. Jepsen op outcome** | `with-errors op idempotent?` (`client.clj:316-330`): a known error becomes `:fail` if definite or if the op's `:f` is in `idempotent?`, else `:info` | This is not a retry; it is the correct answer. `:info` = "may have happened", and Knossos/Elle treat it as such. | **Keep.** Pass `#{:read}` as the idempotent set (as `cas_register.clj:45` does), never `#{}` plus a write: a timed-out write marked `:fail` claims it did not happen, and on SC it may have — that is a false positive waiting to happen. Reads may be `:fail`; writes on timeout must be `:info`. |
| **3. Jepsen setup** | `retry-each` / `wrap-retry` (`client.clj:350+`, 10 tries, 1 s) around `CREATE KEYSPACE/TABLE` in each workload's `setup!` | Re-runs DDL while the cluster settles. Harmless: DDL is idempotent and outside the history. | **Keep for DDL only.** A grep for `retry-each` in `scylla/sc/` must match only `setup!` bodies — add that grep to step 1.4's check. |
| **4. `aggressive-read`** | `client.clj:172-190`: up to 100 read retries, 2 s sleeps, meant for the *final* read after the nemesis is healed | A read never changes state, so retrying it is sound; it only hides availability problems. | **Allowed only for `:final-read` ops** of the batch-isolation workload (the only SC workload with a final read). Elle workloads have none. |
| **5. SCT job** | `jepsen_test_count` × `run_jepsen_cmd_any` (§2.5) | Re-runs whole tests until one is green. | **Removed** (steps 1.2, 1.5, 3.2). |

The rule in one line: **a write is sent exactly once, its outcome is `:ok`, `:fail` or `:info`,
and nothing below or above the history re-sends it.** Everything that is not a write (DDL, reads,
the SCT job) may be retried only where the table says so.

## 3. Goals

1. A Jepsen job that runs SC workloads on a real cluster and fails when linearizability or
   single-partition transaction isolation is violated — proven with an injected-anomaly canary.
2. Faults that sc-verify does not give us: clock skew (bump and strobe), Jepsen-driven partitions
   correlated with the history, membership changes (decommission / remove / re-add) from the
   test's point of view, plus Elle anomaly names (G1c, G-single, lost update, …) instead of
   "not linearizable".
3. A real gate: all tests must be `:valid? true`; `:unknown` is a warning; infrastructure failures
   are reported as such, not as consistency failures.
4. Results in Argus: one row per (workload, nemesis) with validity, op counts and anomaly types,
   plus the archived history link.
5. Every step verifiable by an agent locally (Docker) before any cloud run.

Out of scope: performance numbers (SCT perf jobs own that), released branches (SC is master-only,
`2026.4.0-dev`), replacing sc-verify.

## 4. Revive or rewrite?

**Keep the plumbing, replace the test content — as a new namespace tree `scylla.sc.*` next to
the old one, never by editing the old namespaces.** The SCT side (pipeline, tester, config mixin,
collector, viewer) is small and is fixed in place. In the `scylladb/jepsen` fork (3,220 lines):

| Part | Lines | Decision | Why |
| --- | --- | --- | --- |
| `client.clj` — driver wrapper, exception → `:fail`/`:info` mapping, retry policies | 400 | **Copy once to `scylla.sc.client`, trim** | The hard-won part: which Scylla exception means "did not happen" vs "don't know". Trim per §2.6. |
| `nemesis.clj` — pause, kill, partition, clock, membership via nodetool | 422 | **Keep as-is** | The reason to use Jepsen at all. Nothing in it is LWT-specific. |
| `db.clj` — apt install, version switching, local-deb upload, `scylla.yaml` template, wipe, start/stop/kill, log paths | 518 | **Replace with ~80 lines `scylla.sc.db`** | 80% exists so Jepsen can *install and re-form* the cluster, which is the source of the flakiness (§2.5). The new db knows only start / stop / kill / pause and log paths. |
| `core.clj` — CLI, `test-all`, nemesis matrix | 329 | **Keep, add `test-all-sc`** | |
| `list_append.clj`, `wr_register.clj` | 543 | **Copy to `scylla.sc.*`, strip LWT** | The Elle generator and checker wiring survive; ~20 lines of `IF lwt_trivial = null` and `:serial` per file go. |
| `write_isolation.clj` | 119 | **Copy, check for `IF`** | Single-partition BATCH; may run on SC unchanged (step 2.4). |
| `cas_register`, `counter`, `cmap`, `cset`, `mv`, `batch*` | ~640 | **Leave untouched; delete in 5.3** | LWT, counters, collections, MV: no SC future. |
| `checker.clj`, `conductors.clj` | 139 | Keep | |

Net: ~1,300 lines kept untouched, ~400 copied and trimmed, ~500 new, ~640 left alone until
deleted. The old namespaces stay runnable by the two existing jobs until 5.3, so no step can
break a previous one and every agent step has a clear file boundary.

Why not a greenfield test: `client.clj` and `nemesis.clj` are exactly what a fresh
`lein new jepsen` would make an agent re-derive from Scylla's exception zoo, and that is where a
rewrite introduces *new* unsoundness (a retried write silently invalidates the history). Why not
just elle-cli on sc-verify histories: that gives the checkers but none of the nemeses (goal 2);
§7 lists it as the fallback if the humans decide the nemeses are not wanted.

Jepsen version: **bump 0.2.4 → 0.3.x first (step 1.0).** The local run (§2.3 item 10) showed
0.2.4 cannot set up a Debian 12 node at all; 0.3.x fixes that upstream, and the new
`scylla.sc.db` is the one place the API difference bites — cheaper to write it against 0.3.x once
than to port it later. The old namespaces must still compile after the bump (they are not run).

## 5. Implementation Phases

Rules for every step: the check is a command; the DoD is what its output must contain. A step that
needs a cloud cluster also has a local check that runs first. SC is enabled with
`experimental_features: [strongly-consistent-tables]` and keyspaces created with
`… AND consistency = 'global' AND tablets = {'initial': N}` (as in the sc-verify config and
[PR 16167](https://github.com/scylladb/scylla-cluster-tests/pull/16167)).

### Which agent runs which step

| Steps | Model | Why |
| --- | --- | --- |
| 1.0, 1.2–1.4, all of Phase 2, 3.2–3.4 | Opus 5.5 (or Fable 5.1), high effort | Clojure is a low-resource language for models, and the mistakes that matter are silent: a `:fail` where it should be `:info`, a driver retry left on. The canary catches "checker never says no"; only careful reading catches "says no for the wrong reason". |
| 1.1, 1.5, 3.1, Phase 4, 5.2, 5.3, docs | Sonnet 5.5 | Python/YAML with an existing pattern to copy and a unit test as the check. |
| Phase 0, 5.1 soak triage | Sonnet 5.5 | Reading and tabulating. |

Two rules that matter more than the model:

1. **Every Clojure change gets a second, fresh-context review agent** (same or stronger model),
   given the diff, §2.6 and the SC restriction table — not the author's conclusions. An author
   agent confirms its own hypothesis; a cold reader does not.
2. **The author agent runs tier A itself before opening a PR.** "It compiles" is never the DoD;
   `:valid? true` on SC plus `:valid? false` on the EC canary is.

### Local debugging environments

Jepsen needs root ssh into every DB node, `sudo`, `systemctl`/`service scylla-server`, `iptables`,
and (for `:clock`) `cc` + `ntpdate`. That rules one option out and leaves two:

| Tier | Environment | Runs | Verifies | Host |
| --- | --- | --- | --- | --- |
| **A — Clojure only** | [docker/jepsen-local/](../../../docker/jepsen-local/) — Jepsen's own Docker env (`jepsen-io/jepsen@50de29bb` `docker/`: Debian 12 nodes with systemd, sshd, `build-essential`, `ntpdate`, `faketime`, `iptables`; one control node with lein) with four patches: arm64-safe JDK, Scylla master baked into the node image, dummy `netcat` + tcpdump symlink (§2.3 items 10–11), ed25519 key (§2.3 item 12). How to run: [docs/jepsen-sc.md](../../jepsen-sc.md). **Proven 2026-11-03**: `--workload none` passes on this Mac in ~30 s after the images are built. | `lein run test …` from the control container against 3–5 node containers with Scylla `master:latest`, `developer_mode`, `--memory 2G`, host `fs.aio-max-nr` raised. | Phases 1–2 (workloads, canaries, nemeses incl. clock). | Any machine with Docker, including this Mac. Nothing from SCT is involved. |
| **B — SCT end to end** | [minicloud](../../minicloud.md): real QEMU/KVM VMs behind an EC2/GCE API emulator; SCT runs unchanged (`hydra run-test jepsen_test --backend aws` + `SCT_MINICLOUD_ENDPOINT_URL`). Loader VM runs lein exactly as on GCE. Downscale with the `downscaling-for-minicloud` skill: 3 DB + 1 loader, no monitor, 4 GiB guests. | `jepsen_test.py`, the new test case, `results.edn` parsing, events, Argus table, collector, viewer. | Phases 3–4 (except 3.5). | **KVM Linux host only** (`/dev/kvm`): the lab agent `minicloud-kvm-builders-v1` or a Linux dev box. Not this Mac. |
| ✗ SCT `docker` backend | DB nodes are containers driven by `DockerCmdRunner` (`docker exec`), no sshd ([sdcm/cluster_docker.py:131-132](../../../sdcm/cluster_docker.py)). Jepsen cannot reach them. Not used. | | | |

Step 0.3 proves tier A; step 3.1's check proves tier B. Each later step names its tier.
Needs Investigation for tier A: whether `developer_mode` must go into the node's `scylla.yaml`
by the compose setup script (after 1.2 Jepsen no longer writes it); for tier B: that minicloud
loader guests reach the internet for `lein deps` and `git clone`.

### Phase 0 — Establish the baseline (no code changes)

- [ ] **0.1 Record what runs today.** Pull the last 10 builds of both Jenkins jobs and their Argus
  runs: Scylla version, result, which `jepsen_test_cmd` runs passed, duration.
  Check: a table in `docs/plans/testing/jepsen-baseline.md`. DoD: table has 10 rows per job or
  states "job never ran since <date>". *Needs human access if Jenkins credentials are not available
  to the agent — see §7.*
- [x] **0.2 Reproduce the loader bootstrap.** Run `setup_jepsen()`'s commands in a `debian:12`
  container. Check: `apt-get install -y openjdk-11-jre` exit code. DoD: documented pass or the
  exact failing package; if it fails, step 1.1 changes the package to `openjdk-17-jre-headless`. **Done 2026-11-03** — in `debian:12`: `E: Package 'openjdk-11-jre' has no installation candidate` (same for `-headless`); `openjdk-17-jre-headless` 17.0.21 installs, exit 0. Step 1.1 switches the package.
- [x] **0.3 Bring up tier A** and run the smoke test from the control node:
  `lein run test --workload none --nemesis none --no-install-scylla --time-limit 10 --node n1 …`.
  Check: `store/latest/results.edn` exists. DoD: `:valid? true` for `none`; `docs/jepsen-sc.md`
  with the exact commands that worked. **Done 2026-11-03** — six attempts, each failure is now a
  numbered item in §2.3 (10–12) and a patch in `docker/jepsen-local/`.
- [x] **0.4 Run one existing workload on an SC keyspace** (expect failure):
  `lein run test --workload cas-register …` with a keyspace created as `consistency='global'`.
  Check: `grep -c InvalidRequest store/latest/jepsen.log`. DoD: >0, confirming §2.2. **Done 2026-11-03** — cas-register on `consistency = 'global'`, 3 nodes, `--concurrency 6` (the workload needs 2n): the grep term is wrong, the Java driver raises `InvalidQueryException`, `grep -c InvalidQueryException jepsen.log` = 102. Two messages, 50 each: `Strongly consistent updates don't support conditions` (every `IF`) and `Strongly consistent reads must use QUORUM/LOCAL_QUORUM or ONE/LOCAL_ONE consistency level` (every `:serial` read). Confirms §2.2.

### Phase 1 — Make the harness obey SCT (fork `scylladb/jepsen`, SCT tester)

- [x] **1.0 Jepsen 0.3.x.** Bump `project.clj` (Jepsen, Clojure 1.11, Elle comes with it), fix
  compile errors in the namespaces that are kept (§4 table); the to-be-deleted workloads only need
  to compile. Tier A. Check: `lein check` exits 0, then the 0.3 smoke test (`--workload none`)
  passes **without** the dummy `netcat` package (remove it from `docker/jepsen-local/node/Dockerfile`
  in the same change). DoD: both; `lein run test-all --help` still lists the old workloads. **Done 2026-11-03** — **0.3.10, not newer**: from 0.3.11 the published jar is AOT-compiled on JDK 21 (`java.util.SequencedCollection` in its rrb-vector classes) and Debian 12 has no JDK 21 package, not even in backports; 0.3.10 is the last jar that loads on JDK 17. Clojure is 1.12.3 (what Jepsen pins), not 1.11. Also needed: slf4j-api 2.0.17 pinned (logback 1.5), membership `State` moved to `jepsen.nemesis.membership.state` with `setup!`/`teardown!`, the Elle 0.1 monkey-patch in `wr_register.clj` removed, dead `conductors.clj` deleted (never required; referenced a non-existent `scylla.db/nodetool`). `lein check` 0; `--workload none` `:valid? true` on a node image without the dummy `netcat`; `test-all --help` lists the workloads. Fork branch `sc-harness`.
- [x] **1.1 Fix loader bootstrap** per 0.2 (`jepsen_test.py:38`). Check: `uv run pytest
  unit_tests/unit/ -k jepsen -q` plus 0.2's container command. DoD: both green. **Done 2026-11-03** — `openjdk-17-jre-headless` (the non-headless `openjdk-11-jre` had no reason to exist on a loader). `debian:12`: `apt-get install -y openjdk-17-jre-headless libjna-java gnuplot graphviz git` exit 0, `java -version` 17.0.21; `uv run pytest unit_tests/unit/ -k jepsen -q`: 15 passed.
- [x] **1.2 `--no-install-scylla` must leave the cluster alone.** In `db.clj`, skip `configure!`,
  `wipe!` and the log deletion when `(:install-scylla test)` is false; keep `start!`/`kill!`/
  `pause!` (the nemesis needs them). Check: put a marker comment in `/etc/scylla/scylla.yaml` and a
  marker file in `/var/lib/scylla/data/` on each local node, run the 0.3 smoke test, then
  `grep marker /etc/scylla/scylla.yaml && ls /var/lib/scylla/data/marker` on each node.
  DoD: both present on all nodes after the run; `results.edn` `:valid? true`. **Done 2026-11-03** — as the new `scylla.sc.db` (§4), selected by `--no-install-scylla` in `core.clj`; `db.clj` untouched. The marker file must be `chown scylla:scylla`: Scylla refuses to start on a root-owned file in its data directory (`File not owned by current euid`). Tier A needed the nodes to configure themselves at boot (`docker/jepsen-local/node/configure-scylla.sh`: addresses, seeds, SC flag, `--smp 2`), which answers the §5 "Needs Investigation" for `developer_mode` (it lives in `/etc/scylla.d`, not in `scylla.yaml`). Jepsen kills the DB before collecting logs unless `--leave-db-running`; config and data survive. Run: `Scylla up in 6.7 s` per node, `:valid? true`, both markers present on n1–n3.
- [x] **1.3 SC-aware schema setup.** Add a `--sc` flag (default on for `scylla.sc.*` workloads)
  that creates the keyspace with `consistency = 'global' AND tablets = {'initial': 32}` and
  `NetworkTopologyStrategy`, RF 3. Check: `cqlsh -e "DESCRIBE KEYSPACE jepsen_sc"` on a node after
  a run. DoD: output contains `consistency = 'global'`. **Done 2026-11-03** — `--[no-]sc` in `core.clj` (default on); `scylla.sc.db` creates `jepsen_sc` on the last node to come up (RF 3 with tablets needs all nodes). `cqlsh n1 -e "DESCRIBE KEYSPACE jepsen_sc"` → `... AND consistency = 'global' AND tablets = {'enabled': true, 'initial': 32}`. The check must name the node's address: a bare `cqlsh` targets 127.0.0.1, which the nodes do not listen on. A node restarted alone does not open CQL (Raft group 0 needs a majority), so start all nodes first.
- [x] **1.4 Retries, per §2.6.** In `scylla.sc.client`: `never-retry` hard-wired (no `--retry`
  option), `setIdempotent(false)` on every statement, speculative execution off; `with-errors`
  called with `#{:read}` everywhere; `retry-each` only inside `setup!`; `aggressive-read` only on
  `:final-read`. Check: a unit test in the fork that opens the SC cluster options and asserts the
  retry-policy class and the statement idempotence flag, plus
  `grep -n "retry-each\|aggressive-read\|with-errors" src/scylla/sc/*.clj` reviewed against the
  §2.6 table. DoD: test green; every grep hit is in an allowed place; `grep -c "nbRetry > 100"`
  on `scylla/sc/client.clj` is 0. **Done 2026-11-03** — `scylla.sc.client`: `never-retry` hard-wired, `statement` sets `setIdempotent(false)` on every statement (alia's `:idempotent?` only sets the flag when true, so it is done on the Statement directly), server-side timestamps (SC rejects client timestamps; the fuzz/quantize generators are gone), `with-errors` has no idempotent-set parameter any more (`#{:read}` is built in), `aggressive-read` dropped until the batch-isolation workload needs a bounded one. `lein test :only scylla.sc.client-test`: 2 tests, 9 assertions, 0 failures. Greps: `retry-each` used once, in `scylla.sc.db/create-keyspace!` (DDL, called from `setup!`); `nbRetry > 100` count 0.
- [x] **1.5 Gate policy.** Set `jepsen_test_count: 1`, `jepsen_test_run_policy: all` in the SC
  test case (new file, step 3.1). Keep the old YAMLs untouched until step 5.3 decides their fate.
  Check: `uv run pytest unit_tests/unit/config -k jepsen -q`. DoD: green. **Done 2026-11-03** — `test-cases/features/strong-consistency/jepsen-sc.yaml` with `jepsen_test_count: 1`, `jepsen_test_run_policy: all`, SC feature flag, 5 nodes; `test_metadata` and the taxonomy entries stay with 3.1 (`jepsen` is not a `stress_tools` value yet, `strong-consistency` not a `features` value). `uv run pytest unit_tests/unit/config -k jepsen -q`: 8 passed; `sct.py lint-yamls -b gce -i jepsen-sc`: clean.

### Phase 2 — SC workloads (fork, new namespaces `scylla.sc.*`)

Each workload ships with a **canary**: the same workload run against an EC keyspace
(`consistency` unset, RF 3, reads at `CL=ONE`, writes at `CL=ONE`) under `[:partition]` must
produce `:valid? false`. A checker that never says "no" is not a checker. Each workload also uses
`jepsen.independent` (keys minted once, ~100–200 ops per key, never reused) — the same conclusion
the Porcupine research reached in §5.3, and what keeps Knossos/Elle fast.

- [ ] **2.1 `sc-register`.** One row per key: `INSERT`/`UPDATE … SET v = ?` (no `IF`), `SELECT v`.
  Checker: `checker/linearizable` with `model/register`, wrapped in `independent/checker`.
  Writes that time out stay `:info` (indeterminate) — Knossos handles that natively.
  Check: `lein run test --workload sc-register --nemesis none --time-limit 60 --concurrency 5n`
  on the local SC cluster, then `grep ':valid? true' store/latest/results.edn`; then the canary.
  DoD: SC run `:valid? true`; EC canary `:valid? false` with a Knossos counterexample in `results.edn`.
  This duplicates Porcupine's check on purpose: an independent checker on the same semantics is
  the cheapest cross-validation available, and it costs one file.
- [ ] **2.2 `sc-wr-register`.** Elle rw-register. Keys are clustering rows inside one partition
  (`pk` fixed per generation, `ck` = register id); a transaction of several reads/writes is one
  single-partition `BATCH` (writes) or one `SELECT … WHERE pk = ? AND ck IN (…)` (reads).
  Checker: `elle.rw-register` with `:consistency-models [:strict-serializable]`
  `:linearizable-keys? true`. This is what Porcupine does not check: atomicity and isolation of a
  multi-row batch inside one partition (research §6 V6: "the only way to check single-partition
  BATCH atomicity"). Check and DoD as 2.1, plus the canary must name an anomaly type (`G1c`,
  `G-single`, `lost-update`, …) in `results.edn`.
- [ ] **2.3 `sc-list-append`.** Elle list-append on `list<bigint>`: `UPDATE t SET v = v + [?]`
  (no `IF`), `SELECT v`. Checker `elle.list-append`, `:strict-serializable`.
  Two unknowns this workload settles (research §13): are collections accepted on SC tables, and
  does list element order follow Raft commit order. Check: as 2.1. DoD: either `:valid? true` on
  SC and `:valid? false` on the canary, **or** a written finding "lists on SC: <error>" and the
  workload marked skipped — both outcomes count as done; the second is a Scylla ticket (§7).
- [ ] **2.4 `sc-batch-isolation`.** Port `write_isolation.clj` single-row variant: a BATCH writes
  the same value to N rows of one partition; a read of all rows must see one value. Checker: the
  existing write-isolation checker. Check: `grep -n "only-if\|serial" src/scylla/sc/batch_isolation.clj`
  empty; run as 2.1. DoD: SC `:valid? true`, EC canary `:valid? false`.
- [ ] **2.5 Bank — not applicable, write that down.** A bank transfer is a read-modify-write that
  needs CAS or a counter to be atomic; SC offers neither (LWT rejected, counters rejected,
  `SET v = v - ?` is counter-only). On a correct SC system two concurrent transfers lose an update
  and the invariant breaks — the test cannot distinguish a bug from the model. Check: one paragraph
  in the fork's README. DoD: paragraph present; no `sc-bank` namespace.
- [ ] **2.6 `test-all` for SC.** New CLI entry `test-all-sc`: workloads `{sc-register,
  sc-wr-register, sc-list-append, sc-batch-isolation}` × `standard-nemeses`, `--time-limit 120`.
  Check: `lein run test-all-sc --nemesis none --time-limit 20 …` runs 4 tests, `results.edn` for
  each. DoD: 4 directories under `store/`, all `:valid? true`.

### Phase 3 — SCT test case, nemesis, gate

- [ ] **3.1 New test case** `test-cases/features/strong-consistency/jepsen-sc.yaml`: 5 DB nodes,
  `master:latest` only, `experimental_features: [strongly-consistent-tables]`, `bare_loaders: true`
  (Jepsen is the loader; keep), `NoOpMonkey` (Jepsen is the nemesis; keep), `jepsen_test_cmd:
  ["test-all-sc --concurrency 5n --time-limit 300"]`, `jepsen_test_count: 1`, policy `all`, full
  `test_metadata` (`test_type: jepsen`, `features: [tablets, strong-consistency]`,
  `stress_tools: [jepsen]`, `team_ownership: core-qa`). Add `jepsen` to
  `docs/pipeline-labels/taxonomy.yaml` if missing.
  Check: `uv run pytest unit_tests/unit/test_test_metadata.py unit_tests/unit/config -q` and
  `utils/lint_test_cases.sh`. DoD: green.
- [ ] **3.2 Parse `results.edn` instead of exit codes.** `jepsen_test.py`: after each command, pull
  `store/*/results.edn` from the loader, parse `:valid?` per test (true / false / `:unknown`).
  `false` → `JepsenEvent(severity=CRITICAL)` naming workload, nemesis, anomaly types; `:unknown` →
  WARNING; a missing `results.edn` → ERROR "infrastructure" (lein/ssh/deps failure). Register the
  event in `LOADERS_EVENTS`. Check: `uv run pytest unit_tests/unit/test_jepsen_results.py -q` with
  three fixture `results.edn` files (valid, invalid with Elle anomalies, unknown). DoD: green;
  `test_jepsen()` no longer uses `.ok` of the remoter result as the verdict.
- [ ] **3.3 Clock nemesis prerequisites.** Jepsen's `:clock` nemesis compiles `bump-time.c` on each
  node and uses `ntpdate`/`date`. Check on a local node: `which cc ntpdate`. DoD: either both found
  on the SCT GCE image, or `setup_jepsen()` installs `build-essential ntpdate` on DB nodes (as it
  already does for `tcpdump`, `jepsen_test.py:56-58`) and a `--nemesis clock --time-limit 60` run
  ends `:valid? true` with a clock plot in `store/latest/`.
- [ ] **3.4 Membership nemesis against SCT-provisioned nodes.** `:membership` decommissions and
  re-adds nodes via `nodetool` and re-bootstraps with Jepsen's own seeds (`db.clj:329-340`). After
  1.2 this must use the node's existing `scylla.yaml`. Known SC blocker: topology changes wait on
  snapshot transfer (SCYLLADB-4753). Check: `--nemesis membership --time-limit 120` on the local
  cluster. DoD: run completes with a verdict (valid or an SC bug reference), or the nemesis is
  excluded from `test-all-sc` with a comment naming SCYLLADB-4753.
- [ ] **3.5 First cloud run.** Trigger the new job on GCE with `master:latest`.
  Check: Argus run exists, `jepsen-data` archive in S3, SCT log has one `JepsenEvent` per test.
  DoD: all tests `:valid? true` or each failure filed as a Scylla ticket with the `results.edn`
  counterexample attached. *Requires cloud credentials — §7.*

### Phase 4 — Argus and artifacts

- [ ] **4.1 Argus table.** `StaticGenericResultTable` "Jepsen SC": one row per (workload, nemesis)
  with `valid`, `ok_count`, `fail_count`, `info_count`, `anomalies` (comma-joined), `duration_s`;
  `ValidationRule(fixed_limit=0)` on invalid count. Submit from `test_jepsen()` (nothing calls
  `submit_results_to_argus` unless the tester does — research §10.5). Check: `uv run pytest
  unit_tests/unit/test_argus_results.py -k jepsen -q`. DoD: green; row visible in the 3.5 Argus run.
- [ ] **4.2 Fix the viewer.** `sdcm/utils/jepsen.py:82-83`: download by the archive's real name and
  extract with `tar --zstd` (or `tar -xaf`). Check: `hydra investigate show-jepsen-results <test_id>`
  against the 3.5 run. DoD: the Jepsen web UI opens and shows the runs.
- [ ] **4.3 Save the real report.** `save_jepsen_report()` (`jepsen_test.py:93-110`) must save
  `store/<test>/results.edn` and `history.txt` per test to `logdir`, not the server index page.
  Check: `ls logdir/jepsen/*/results.edn` after a local run through `hydra run-test` on the docker
  backend. DoD: one file per test.
- [ ] **4.4 Collector keeps only what matters.** `JepsenLogCollector` archives the whole repo clone
  (`~/jepsen-scylla`, incl. `.git` and `target/`). Archive `store/` only. Check: archive size and
  `tar --zstd -tf` listing. DoD: listing contains `store/` and no `target/`.

### Phase 5 — Hardening and clean-up

- [ ] **5.1 Soak.** Run the job 5 times on consecutive days at `--time-limit 600`. Check: Argus rows.
  DoD: 5 runs, every failure is a Scylla ticket or a harness ticket, none "unknown cause".
  Only after this does the job become a release gate (same rule the Porcupine plan applies to itself).
- [ ] **5.2 Fate of the old jobs.** Either delete `test-cases/jepsen/*.yaml`, both old jenkinsfiles
  and the LWT defaults in `test_default.yaml:181-188`, or re-point them at `test-all-sc`. Decision
  in §7. Check: `grep -rn "jepsen_with_raft\|jepsen-all" jenkins-pipelines test-cases` empty after
  deletion. DoD: `uv run pytest unit_tests/unit/lint unit_tests/unit/test_staging_trigger.py -q` green.
- [ ] **5.4 Docs.** `docs/configuration_options/jepsen-tests.md` regenerated; a short
  `docs/jepsen-sc.md`: how to run locally, how to read `results.edn`, how to add a workload.
  Check: `uv run python -m sdcm.sct_config --help` (or the repo's doc generator) exits 0.

## 6. Success Criteria

- The job fails on an injected anomaly (EC canary) and passes on SC under all six nemesis sets.
- Argus shows one row per (workload, nemesis) with anomaly names; the history archive opens in the
  viewer.
- Five consecutive clean or fully-explained runs before the job gates a release.
- At least one of: a Scylla bug found, or a written statement of which SC claim each workload
  proves (register linearizability, batch atomicity, list order) that sc-verify does not.

## 7. Risks, Open Questions and Decisions for Humans

Risks:
- **Clojure skills.** Nobody on the team maintains this code; agents will. Mitigation: every change
  has a local run as its check; no change without one.
- **Knossos cost** is O(c!) in per-key concurrency (research §9). Mitigation: `jepsen.independent`,
  ≤5 concurrent clients per key, `--time-limit` per test; `:unknown` is a warning.
- **`--no-install-scylla` semantics change (1.2)** may break jepsen-io-style local runs where Jepsen
  does install Scylla. Keep both paths; the flag selects.
- **Topology nemeses blocked** by SCYLLADB-4753 until snapshot transfer lands; the job starts
  without `:membership`.
- **Cloud budget / credentials.** Steps 0.1, 3.5, 5.1 need Jenkins and GCE access an agent may not
  have.

Open questions (need a human answer before the step they block):
1. Should the Jepsen job be a gate at all, or a nightly like `test.py`'s Porcupine test? (blocks 5.1)
2. Is the sc-verify history format to be fed to the same Elle checkers (elle-cli) as a cheaper way
   to get goal 2's anomaly names? If yes, 2.2/2.3 still stand (they add the nemeses), but 2.1 can be
   dropped. (blocks 2.1)
3. Fate of `jepsen-all` and `jepsen-with-raft` jobs and the LWT workloads: delete, or keep as an
   EC/LWT regression job with the fixed gate? (blocks 5.3)
4. Is list element order on SC tables a committed guarantee? If not, 2.3 becomes a documentation
   test, not a correctness test. (blocks 2.3's interpretation)
5. Who owns the `scylladb/jepsen` fork for merges? Agents can open PRs; someone must have merge
   rights. (blocks Phase 1)
6. Jira: SCYLLADB-42's scope says "investigate"; this plan implements. Confirm the ticket (or a new
   one) is the right place to track Phases 1–5, and whether 2027.1 (`SCYLLADB-5130`) is the target.
