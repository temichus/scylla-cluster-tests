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
# Copyright (c) 2026 ScyllaDB

"""results.edn of real local runs (plan step 3.2): a valid sc-register run, its EC canary with a
Knossos counterexample, and the sc-wr-register EC canary with Elle anomalies."""

import pytest

from sdcm.sct_events import Severity
from sdcm.utils.jepsen import JepsenTestResult, jepsen_results, parse_edn

from jepsen_test import jepsen_event


def result(test_data_dir, fixture, name="sc-register lwt wc:one rc:one partition"):
    return JepsenTestResult.from_results(name, parse_edn((test_data_dir / "jepsen" / fixture).read_text()))


def test_parse_edn():
    assert parse_edn('{:a 1, :b [1 2.5 nil true "x\\"y"], :c (:k), :d #{1}, :e #ns.Tag{:f -3N}, 0 :unknown}') == {
        "a": 1,
        "b": [1, 2.5, None, True, 'x"y'],
        "c": ["k"],
        "d": [1],
        "e": {"f": -3},
        0: "unknown",
    }


def test_valid_run(test_data_dir):
    res = result(test_data_dir, "sc-register-valid.edn", name="sc-register lwt")
    assert res.valid is True
    assert (res.ok_count, res.fail_count, res.info_count) == (601, 0, 0)
    assert res.anomalies == []
    assert res.row == "sc-register / none"
    assert jepsen_event(res).severity == Severity.NORMAL


def test_knossos_counterexample(test_data_dir):
    res = result(test_data_dir, "sc-register-canary-knossos.edn")
    assert res.valid is False
    assert (res.ok_count, res.fail_count, res.info_count) == (593, 11, 0)
    assert res.anomalies == ["linearizable"]
    assert res.row == "sc-register / partition"
    event = jepsen_event(res)
    assert event.severity == Severity.CRITICAL
    assert "linearizable" in event.message


def test_elle_anomalies(test_data_dir):
    res = result(test_data_dir, "sc-wr-register-canary-elle.edn", name="sc-wr-register lwt wc:one rc:one partition")
    assert res.valid is False
    assert res.info_count == 7
    assert res.anomalies == ["G-single-item-realtime", "cyclic-versions"]
    assert jepsen_event(res).severity == Severity.CRITICAL


def test_unknown_is_a_warning():
    res = JepsenTestResult.from_results(
        "sc-register lwt pause,kill", {"valid?": "unknown", "workload": {"valid?": "unknown"}}
    )
    assert res.valid is None
    assert res.nemesis == "pause,kill"
    assert jepsen_event(res).severity == Severity.WARNING


@pytest.mark.parametrize(
    "name, nemesis",
    [("sc-register lwt", "none"), ("sc-register lwt wc:one rc:one", "none"), ("none lwt partition", "partition")],
)
def test_nemesis_from_name(name, nemesis):
    assert JepsenTestResult(name, True).nemesis == nemesis


def test_store_layout(tmp_path, test_data_dir):
    test_dir = tmp_path / "sc-register lwt " / "20261103T152431.657Z"
    test_dir.mkdir(parents=True)
    test_dir.joinpath("results.edn").write_text((test_data_dir / "jepsen" / "sc-register-valid.edn").read_text())
    (results,) = jepsen_results(tmp_path)
    assert results.name == "sc-register lwt" and results.path == test_dir
