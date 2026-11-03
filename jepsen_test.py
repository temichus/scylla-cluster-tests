# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import os
import tarfile
from functools import partial
from pathlib import Path

from sdcm.remote import shell_script_cmd
from sdcm.sct_events import Severity
from sdcm.sct_events.loaders import JepsenEvent
from sdcm.tester import ClusterTester, teardown_on_exception
from sdcm.utils.decorators import log_run_info
from sdcm.utils.git import clone_repo
from sdcm.utils.jepsen import JepsenTestResult, jepsen_results

DB_SSH_KEY = "db_node_ssh_key"
JEPSEN_DIR = "jepsen-scylla"  # the fork clone on the loader; Jepsen writes store/ under it
RESULTS_ARCHIVE = "jepsen-results.tar"


def jepsen_event(result: JepsenTestResult) -> JepsenEvent:
    """results.edn -> event: a violation is CRITICAL, an undecided checker a WARNING."""
    if result.valid is True:
        return JepsenEvent(f"{result.row}: valid", severity=Severity.NORMAL)
    if result.valid is False:
        return JepsenEvent(
            f"{result.row}: consistency violation, anomalies: {', '.join(result.anomalies) or 'see results.edn'}",
            severity=Severity.CRITICAL,
        )
    return JepsenEvent(f"{result.row}: checker result unknown", severity=Severity.WARNING)


class JepsenTest(ClusterTester):
    @property
    def jepsen_node(self):
        return self.loaders.nodes[0]

    @property
    def jepsen_logdir(self) -> Path:
        return Path(self.logdir) / "jepsen"

    @teardown_on_exception
    @log_run_info
    def setup_jepsen(self):
        remoter = self.jepsen_node.remoter
        remoter.sudo(
            "apt-get install -o DPkg::Lock::Timeout=300 -y openjdk-17-jre-headless libjna-java gnuplot graphviz git"
        )
        remoter.run(
            shell_script_cmd("""\
            curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
            chmod +x lein
            ./lein
        """)
        )
        clone_repo(
            remoter=remoter,
            repo_url=self.params.get("jepsen_scylla_repo"),
            destination_dir_name=JEPSEN_DIR,
            clone_as_root=False,
        )
        for db_node in self.db_cluster.nodes:
            remoter.run(f"ssh-keyscan -t ed25519 {db_node.ip_address} >> ~/.ssh/known_hosts")

            # newer jepsen test is using tcpdump, and expect it in /usr/bin
            db_node.install_package("tcpdump")
            db_node.remoter.sudo("ln -s /usr/bin/tcpdump /usr/sbin/tcpdump", ignore_status=True)

        remoter.send_files(os.path.expanduser(self.db_cluster.nodes[0].ssh_login_info["key_file"]), DB_SSH_KEY)

    def setUp(self):
        super().setUp()
        self._jepsen_seen: set[Path] = set()
        self.setup_jepsen()

    def fetch_jepsen_results(self) -> list[JepsenTestResult]:
        """Copies every test's results.edn and history.txt from the loader's store/ to
        logdir/jepsen/<test name>/<timestamp>/ and returns the tests not seen before."""
        remoter = self.jepsen_node.remoter
        # Test names contain spaces; `latest` and `current` are symlinks and skipped.
        remoter.run(
            f"cd ~/{JEPSEN_DIR} && find store -mindepth 3 -maxdepth 3 \\( -name results.edn -o -name history.txt \\)"
            f" -print0 | tar --null -T - -cf ~/{RESULTS_ARCHIVE}",
            ignore_status=True,
        )
        self.jepsen_logdir.mkdir(parents=True, exist_ok=True)
        remoter.receive_files(src=RESULTS_ARCHIVE, dst=str(self.jepsen_logdir))
        with tarfile.open(self.jepsen_logdir / RESULTS_ARCHIVE) as archive:
            for member in archive.getmembers():
                member.name = member.name.removeprefix("store/")
            archive.extractall(self.jepsen_logdir, filter="data")
        new = [result for result in jepsen_results(self.jepsen_logdir) if result.path not in self._jepsen_seen]
        self._jepsen_seen.update(result.path for result in new)
        return new

    def run_jepsen_cmd(self, jepsen_cmd) -> bool:
        """Runs one lein command and judges it by the results.edn of the tests it wrote,
        never by its exit code: a violation and a broken harness are different failures."""
        self.jepsen_node.remoter.run(jepsen_cmd, ignore_status=True, verbose=True)
        results = self.fetch_jepsen_results()
        if not results:
            JepsenEvent(f"infrastructure: no results.edn written by `{jepsen_cmd}'", severity=Severity.ERROR).publish()
            return False
        for result in results:
            jepsen_event(result).publish()
        return all(result.valid is not False for result in results)

    def iter_jepsen_cmd(self, jepsen_cmd, ntimes=1):
        for ntry in range(1, ntimes + 1):
            self.log.info("Run #%s/%s of Jepsen test: `%s'", ntry, ntimes, jepsen_cmd)
            yield self.run_jepsen_cmd(jepsen_cmd)

    def run_jepsen_cmd_most(self, jepsen_cmd, ntimes):
        return sum(self.iter_jepsen_cmd(jepsen_cmd, ntimes)) << 1 > ntimes

    def run_jepsen_cmd_any(self, jepsen_cmd, ntimes):
        return any(self.iter_jepsen_cmd(jepsen_cmd, ntimes))

    def run_jepsen_cmd_all(self, jepsen_cmd, ntimes):
        return all(self.iter_jepsen_cmd(jepsen_cmd, ntimes))

    def test_jepsen(self):
        nodes = " ".join(f"--node {node.ip_address}" for node in self.db_cluster.nodes)
        creds = f"--username {self.db_cluster.nodes[0].ssh_login_info['user']} --ssh-private-key ~/{DB_SSH_KEY}"
        run_jepsen_cmd = partial(
            getattr(self, f"run_jepsen_cmd_{self.params.get('jepsen_test_run_policy')}"),
            ntimes=self.params.get("jepsen_test_count"),
        )
        passed = True
        for test in self.params.get("jepsen_test_cmd"):
            jepsen_cmd = f"cd ~/{JEPSEN_DIR} && ~/lein run {test} {nodes} {creds} --no-install-scylla"
            passed &= run_jepsen_cmd(jepsen_cmd)
        self.assertTrue(passed, "Some of Jepsen tests were failed")

    def collect_test_artifacts(self):
        # Whatever a killed test left behind; the per-command fetch already saved the rest.
        self.fetch_jepsen_results()
