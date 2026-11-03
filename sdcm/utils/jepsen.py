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
# Copyright (c) 2021 ScyllaDB

import json
import os
import re
import uuid
import logging
from collections.abc import Hashable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from functools import cached_property
from urllib.parse import urlparse

from sdcm.utils.common import list_logs_by_test_id, get_free_port
from sdcm.utils.docker_utils import ContainerManager, Container, DockerException


JEPSEN_IMAGE = "docker.io/scylladb/hydra-loaders:jepsen-20240729"
JEPSEN_RESULTS_PORT = 8080
# The viewer needs a Jepsen project around an archived store/ to run `lein run serve`.
JEPSEN_REPO = "https://github.com/scylladb/jepsen.git"

LOGGER = logging.getLogger(__name__)


# --- results.edn -------------------------------------------------------------------------------

_EDN_TOKEN = re.compile(
    r"""
    (?P<ws>[\s,]+)
  | (?P<str>"(?:[^"\\]|\\.)*")
  | (?P<char>\\(?:newline|space|tab|return|u[0-9a-fA-F]{4}|\S))
  | (?P<set>\#\{)
  | (?P<discard>\#_)
  | (?P<tag>\#[A-Za-z][^\s,()\[\]{}"]*)
  | (?P<open>[(\[{])
  | (?P<close>[)\]}])
  | (?P<atom>[^\s,()\[\]{}"]+)
    """,
    re.VERBOSE,
)
_EDN_CLOSE = {"(": ")", "[": "]", "{": "}", "#{": "}"}


_EDN_LITERALS = {"nil": None, "true": True, "false": False}


def _edn_atom(tok: str):
    if tok in _EDN_LITERALS:
        return _EDN_LITERALS[tok]
    if tok[0] == ":":
        return tok[1:]
    for convert in (lambda t: int(t.rstrip("N")), lambda t: float(t.rstrip("M"))):
        try:
            return convert(tok)
        except ValueError:
            continue
    return tok


def parse_edn(text: str):
    """A reader for the EDN Jepsen writes: enough for results.edn, not a full EDN implementation.

    Keywords and symbols become strings without the colon (``:valid?`` -> ``"valid?"``), maps
    dicts, vectors/lists/sets lists, nil None. A tagged element (``#jepsen.history.Op{...}``)
    is its element. A map key that is not hashable is kept as its repr.
    """
    tokens = [(m.lastgroup, m.group()) for m in _EDN_TOKEN.finditer(text) if m.lastgroup != "ws"]
    pos = 0

    def read():
        nonlocal pos
        kind, tok = tokens[pos]
        pos += 1
        if kind == "str":
            return json.loads(tok, strict=False)
        if kind == "char":
            return tok[1:]
        if kind == "close":
            raise ValueError(f"unexpected {tok!r} at token {pos}")
        if kind in ("open", "set"):
            close, items = _EDN_CLOSE[tok], []
            while tokens[pos][1] != close:
                items.append(read())
            pos += 1
            if tok != "{":
                return items
            return {(k if isinstance(k, Hashable) else repr(k)): v for k, v in zip(items[::2], items[1::2])}
        if kind in ("discard", "tag"):
            if kind == "discard":
                read()
            return read()
        return _edn_atom(tok)

    value = read()
    if pos != len(tokens):
        raise ValueError(f"trailing tokens in EDN at token {pos}")
    return value


def _invalid_checkers(checker: dict) -> list[str]:
    """Names of the invalid checkers nested in a results map: Elle anomaly types where the
    checker lists them, else the checker's own key (``linearizable`` for Knossos)."""
    names = list(checker.get("anomaly-types") or [])
    for key, value in checker.items():
        if not isinstance(value, dict):
            continue
        if "valid?" in value and value["valid?"] is not True and isinstance(key, str) and "anomaly-types" not in value:
            names.append(key)
        names.extend(_invalid_checkers(value))
    return list(dict.fromkeys(names))


@dataclass
class JepsenTestResult:
    """One Jepsen test, as results.edn reports it."""

    name: str  # the Jepsen test name, e.g. "sc-register lwt wc:one rc:one partition"
    valid: Optional[bool]  # None for :unknown (the checker gave up)
    ok_count: int = 0
    fail_count: int = 0
    info_count: int = 0
    anomalies: list[str] = field(default_factory=list)
    path: Optional[Path] = None

    @property
    def workload(self) -> str:
        return self.name.split()[0]

    @property
    def nemesis(self) -> str:
        # scylla.core builds the name as "<workload> [lwt] [wc:X] [wsc:X] [rc:X] [rsc:X] [<faults>]".
        last = self.name.split()[-1]
        if last in (self.workload, "lwt") or re.match(r"^(wc|wsc|rc|rsc):", last):
            return "none"
        return last

    @property
    def row(self) -> str:
        return f"{self.workload} / {self.nemesis}"

    @classmethod
    def from_results(cls, name: str, results: dict, path: Optional[Path] = None) -> "JepsenTestResult":
        valid = results.get("valid?")
        stats = results.get("stats") or {}
        workload = results.get("workload") or {}
        return cls(
            name=name,
            valid=valid if isinstance(valid, bool) else None,
            ok_count=stats.get("ok-count", 0),
            fail_count=stats.get("fail-count", 0),
            info_count=stats.get("info-count", 0),
            anomalies=_invalid_checkers(workload) if workload.get("valid?") is not True else [],
            path=path,
        )

    @classmethod
    def from_dir(cls, test_dir: Path) -> "JepsenTestResult":
        """test_dir is store/<test name>/<timestamp>/ as Jepsen lays it out."""
        results = parse_edn((test_dir / "results.edn").read_text(encoding="utf-8"))
        return cls.from_results(name=test_dir.parent.name.strip(), results=results, path=test_dir)


def jepsen_results(store_dir: Path) -> list[JepsenTestResult]:
    """Every test under a Jepsen store directory, oldest first."""
    return [JepsenTestResult.from_dir(path.parent) for path in sorted(store_dir.glob("*/*/results.edn"))]


class JepsenResults:
    _containers = {}

    name = f"jepsen_results-{uuid.uuid4()!s:.8}"
    tags = {}

    def jepsen_container_run_args(self) -> dict:
        exposed_port = get_free_port(
            ports_to_try=(
                JEPSEN_RESULTS_PORT,
                0,
            )
        )
        return dict(
            image=JEPSEN_IMAGE,
            entrypoint="/bin/cat",
            tty=True,
            name=f"{self.name}-jepsen",
            ports={
                f"{JEPSEN_RESULTS_PORT}/tcp": {
                    "HostIp": "0.0.0.0",
                    "HostPort": exposed_port,
                },
            },
        )

    @cached_property
    def _jepsen_container(self) -> Container:
        return ContainerManager.run_container(self, "jepsen")

    def runcmd(self, command: str, detach: bool = False) -> None:
        LOGGER.info("Execute `%s' inside Jepsen container", command)
        res = self._jepsen_container.exec_run(["sh", "-c", command], stream=True, detach=detach)
        for line in res.output:
            LOGGER.info(line.decode("utf-8").rstrip())
        if res.exit_code:
            raise DockerException(f"{self._jepsen_container}: {res.output.decode('utf-8')}")

    @property
    def jepsen_results_port(self) -> Optional[int]:
        return ContainerManager.get_container_port(self, "jepsen", JEPSEN_RESULTS_PORT)

    @staticmethod
    def get_jepsen_data_archive_link(test_id):
        if jepsen_data := [log["link"] for log in list_logs_by_test_id(test_id) if "jepsen-data" in log["type"]]:
            LOGGER.info("Found Jepsen data archives for %s: %s", test_id, jepsen_data)
            return jepsen_data[-1]
        LOGGER.warning("No any archive with Jepsen data for %s", test_id)
        return None

    def restore_jepsen_data(self, test_id):
        if jepsen_data_link := self.get_jepsen_data_archive_link(test_id):
            LOGGER.info("Restore Jepsen data and download all dependecies.")
            archive = os.path.basename(urlparse(jepsen_data_link).path)  # .tar.zst, .tar.gz for old runs
            self.runcmd(f"wget --no-verbose -O '{archive}' '{jepsen_data_link}'")
            self.runcmd("command -v zstd >/dev/null || (apt-get -qq update && apt-get -qq install -y zstd)")
            # The collector archives store/ only; archives of older runs hold the whole clone.
            self.runcmd(
                f"tar -xaf '{archive}' && if [ ! -d jepsen-scylla ]; then"
                f" git clone --depth 1 {JEPSEN_REPO} jepsen-scylla && mv store jepsen-scylla/; fi"
            )
            self.runcmd("cd jepsen-scylla && lein deps")
            return True
        return False

    def run_jepsen_web_server(self, detach: bool = False) -> None:
        if detach:
            ContainerManager.set_container_keep_alive(self, "jepsen")
        self.runcmd(command="cd jepsen-scylla && lein run serve", detach=detach)

    def __del__(self):
        ContainerManager.destroy_all_containers(self)
