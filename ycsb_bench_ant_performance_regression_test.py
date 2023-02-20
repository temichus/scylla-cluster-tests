#!/usr/bin/env python

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
# Copyright (c) 2022 ScyllaDB

import logging

from performance_regression_test import PerformanceRegressionTest
from sdcm.sct_events.system import InfoEvent


LOGGER = logging.getLogger(__name__)


class YCSBBenchANTPerformanceRegressionTest(PerformanceRegressionTest):

    def run_pre_create_keyspace(self):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for cmd in self.params.get('pre_create_keyspace'):
                session.execute(cmd)

    def run_workload(self, stress_cmd, nemesis=False, sub_type=None):
        self.create_test_stats(sub_type=sub_type, doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue, store_results=False)
        LOGGER.info("Stress results: %s", str(results))

    def test_latency(self):
        """
        Test steps:

        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run WRITE workload with gauss population.
        """
        self.run_pre_create_keyspace()
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        InfoEvent(message="Starting YCSB benchANT").publish()
        self.run_workload(stress_cmd=self.params['stress_cmd'])
