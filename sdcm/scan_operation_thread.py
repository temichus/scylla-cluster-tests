from __future__ import annotations

import logging
import random
import re
import tempfile
import threading
import time
import traceback
from abc import abstractmethod
from dataclasses import dataclass, field, fields
from string import Template
from typing import Optional, Type, NamedTuple, Literal, get_type_hints, get_origin, TYPE_CHECKING
from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout
from cassandra.cluster import ResponseFuture, ResultSet  # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module
from prettytable import PrettyTable
from sdcm import wait
from sdcm.remote import LocalCmdRunner
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent, FullPartitionScanReversedOrderEvent, FullPartitionScanEvent, \
    FullScanAggregateEvent
from sdcm.utils.common import get_table_clustering_order, get_partition_keys

if TYPE_CHECKING:
    from sdcm.cluster import BaseScyllaCluster, BaseCluster, BaseNode

ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout", 'Host has been marked down or removed')
BYPASS_CACHE_VALUES = [" BYPASS CACHE", ""]
LOCAL_CMD_RUNNER = LocalCmdRunner()


# pylint: disable=too-many-instance-attributes
@dataclass
class ConfigFullScanParams:
    mode: Literal['random', 'table', 'partition', 'aggregate', 'table_and_aggregate']
    ks_cf: str = "random"
    interval: int = 10
    page_size: int = 10000
    pk_name: str = 'pk'
    ck_name: str = 'ck'
    data_column_name: str = 'v'
    validate_data: bool = False
    include_data_column: bool = False
    rows_count: int = 5000
    full_scan_operation_limit: int = 300  # timeout for SELECT * statement, 5 min by default
    full_scan_aggregates_operation_limit: int = 60*30  # timeout for SELECT count(* statement 30 min by default

    def __post_init__(self):
        types = get_type_hints(ConfigFullScanParams)
        errors = []
        for item in fields(ConfigFullScanParams):
            expected_type = types[item.name]
            value = getattr(self, item.name)
            if get_origin(expected_type) is Literal:
                if not value in expected_type.__args__:
                    errors.append(
                        f"field '{item.name}' must be one of '{expected_type.__args__}' but got '{value}'")
            elif not isinstance(value, expected_type):
                errors.append(f"field '{item.name}' must be an instance of {expected_type}, but got '{value}'")
        if errors:
            errors = '\n\t'.join(errors)
            raise ValueError(f"Config fullscan params validarion errors:\n\t{errors}")


@dataclass
class FullScanParams(ConfigFullScanParams):
    db_cluster: [BaseScyllaCluster, BaseCluster] = None
    termination_event: threading.Event = None
    fullscan_user: str = None
    fullscan_user_password: str = None
    duration: int = None


@dataclass
class FullScanStats:
    """
    Keeps track of stats for multiple fullscan operations.
    """
    number_of_rows_read: int = 0
    read_pages: int = 0
    scans_counter: int = 0
    time_elapsed: int = 0
    total_scan_time: int = 0
    stats: list[FullScanOperationStat] = field(default_factory=list)

    def get_stats_pretty_table(self) -> PrettyTable | None:
        if not self.stats:
            return None

        pretty_table = PrettyTable(field_names=[field.name for field in fields(self.stats[0])])
        for stat in self.stats:
            pretty_table.add_row([stat.op_type, stat.duration, "\n".join(stat.exceptions), stat.nemesis_at_start,
                                  stat.nemesis_at_end, stat.success, stat.cmd])
        return pretty_table


@dataclass
class FullScanOperationStat:
    """
    Keeps track of stats for a single fullscan operation.
    """
    op_type: str = None
    duration: float = None
    exceptions: list = field(default_factory=list)
    nemesis_at_start: str = None
    nemesis_at_end: str = None
    success: bool = None
    cmd: str = None


class FullScanCommand(NamedTuple):
    name: str
    base_query: Template


class FullScanAggregateCommands(NamedTuple):
    SELECT_ALL = FullScanCommand("SELECT_ALL", Template("SELECT * from $ks_cf$bypass_cache$timeout"))
    AGG_COUNT_ALL = FullScanCommand("AGG_COUNT_ALL", Template("SELECT count(*) FROM $ks_cf$bypass_cache$timeout"))


class FullscanException(Exception):
    """ Exception during running a fullscan"""
    ...


# pylint: disable=too-many-instance-attributes
class ScanOperationThread:
    """
    Runs fullscan operations according to the parameters specified in the test
    config yaml files. Has 4 main modes:
    - random: uses a seeded random generator to generate a queue of fullscan
    operations using all the available operation types
    - table: uses only FullScanOperation
    - partition: uses only FullPartitionScanOperation
    - aggregate: uses only FullScanAggregatesOperation

    Check FullScanParams class for parameters to tweak.

    Note: the seeded random generator is shared between the thread class and
    the operations instances. So if the operations queue is  Op1, Op2, Op3],
    all of them will be initialized with the same random generator instance
    as this thread. This way the CQL commands generated by the instances are
    also replicable.
    """

    def __init__(self, fullscan_params: FullScanParams, thread_name: str = ""):
        self.fullscan_params = fullscan_params
        self.fullscan_stats = FullScanStats()
        self.log = logging.getLogger(self.__class__.__name__)
        nemesis_seed = self.fullscan_params.db_cluster.params.get("nemesis_seed")
        self.generator = random.Random(int(nemesis_seed)) if nemesis_seed else random.Random()
        self._thread = threading.Thread(daemon=True, name=f"{self.__class__.__name__}_{thread_name}", target=self.run)
        self.termination_event = fullscan_params.termination_event

        # create different scan operations objects
        scan_operation_params = {
            'generator': self.generator,
            'fullscan_params': self.fullscan_params,
            'fullscan_stats': self.fullscan_stats}

        full_scan_operation = FullScanOperation(**scan_operation_params)
        full_partition_scan_operation = FullPartitionScanOperation(**scan_operation_params)
        full_scan_aggregates_operation = FullScanAggregatesOperation(**scan_operation_params)

        # create mapping for different scan operations objects,
        # please see usage in get_next_scan_operation()
        self.scan_operation_instance_map = {
            "table_and_aggregate": lambda: self.generator.choice(
                [full_scan_operation, full_scan_aggregates_operation]),
            "random": lambda: self.generator.choice(
                [full_scan_operation, full_scan_aggregates_operation, full_partition_scan_operation]),
            "table": lambda: full_scan_operation,
            "partition": lambda: full_partition_scan_operation,
            "aggregate": lambda: full_scan_aggregates_operation
        }

    def get_next_scan_operation(self):
        '''
        Returns scan operation object depends on 'mode' - Literal['random', 'table', 'partition', 'aggregate']
        Returns: FullscanOperationBase
        '''
        return self.scan_operation_instance_map[self.fullscan_params.mode]()

    def _run_next_scan_operation(self):
        self.fullscan_stats.read_pages = self.generator.choice([100, 1000, 0])
        try:
            scan_op = self.get_next_scan_operation()
            self.log.debug("Going to run fullscan operation %s", scan_op.__class__.__name__)
            scan_op.run_scan_operation()

            self.log.debug("Fullscan operations queue depleted.")

        except Exception as exc:  # pylint: disable=broad-except
            self.log.error(traceback.format_exc())
            self.log.error("Encountered exception while performing a fullscan operation:\n%s", exc)

        self.log.debug("Fullscan stats:\n%s", self.fullscan_stats.get_stats_pretty_table())

    def run(self):
        end_time = time.time() + self.fullscan_params.duration
        self._wait_until_user_table_exists()
        while time.time() < end_time and not self.termination_event.is_set():
            self._run_next_scan_operation()
            time.sleep(self.fullscan_params.interval)
        # summary for Jenkins. may not log because nobody wait the thread.
        # Fullscan stats logs with debug level in each loop
        self.log.debug("Fullscan stats:\n%s", self.fullscan_stats.get_stats_pretty_table())

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)

    def _wait_until_user_table_exists(self, timeout_min: int = 20):
        text = f'Waiting until {self.fullscan_params.ks_cf} user table exists'
        db_node = random.choice(self.fullscan_params.db_cluster.nodes)

        if self.fullscan_params.ks_cf.lower() == 'random':
            wait.wait_for(func=lambda: len(self.fullscan_params.db_cluster.get_non_system_ks_cf_list(db_node)) > 0,
                          step=60, text=text, timeout=60 * timeout_min, throw_exc=False)
            self.fullscan_params.ks_cf = self.fullscan_params.db_cluster.get_non_system_ks_cf_list(db_node)[0]
        else:
            wait.wait_for(func=lambda: self.fullscan_params.ks_cf in (
                self.fullscan_params.db_cluster.get_non_system_ks_cf_list(db_node)
            ), step=60, text=text, timeout=60 * timeout_min, throw_exc=False)


class FullscanOperationBase:
    def __init__(self, generator: random.Random, fullscan_params: FullScanParams, fullscan_stats: FullScanStats,
                 scan_event: Type[FullScanEvent] | Type[FullPartitionScanEvent]
                 | Type[FullPartitionScanReversedOrderEvent] | Type[FullScanAggregateEvent]):
        """
        Base class for performing fullscan operations.
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.fullscan_params = fullscan_params
        self.fullscan_stats = fullscan_stats
        self.scan_event = scan_event
        self.termination_event = self.fullscan_params.termination_event
        self.generator = generator
        self.db_node = self._get_random_node()
        self.current_operation_stat = None

    def _get_random_node(self) -> BaseNode:
        return self.generator.choice(self.fullscan_params.db_cluster.nodes)

    @abstractmethod
    def randomly_form_cql_statement(self) -> str:
        ...

    def execute_query(
            self, session, cmd: str,
            event: Type[FullScanEvent | FullPartitionScanEvent
                        | FullPartitionScanReversedOrderEvent]) -> ResultSet:
        # pylint: disable=unused-argument
        self.log.debug('Will run command %s', cmd)
        return session.execute(SimpleStatement(
            cmd,
            fetch_size=self.fullscan_params.page_size,
            consistency_level=ConsistencyLevel.ONE)
        )

    def run_scan_event(self, cmd: str,
                       scan_event: Type[FullScanEvent | FullPartitionScanEvent
                                        | FullPartitionScanReversedOrderEvent] = None) -> FullScanOperationStat:
        scan_event = scan_event or self.scan_event
        cmd = cmd or self.randomly_form_cql_statement()
        with scan_event(node=self.db_node.name, ks_cf=self.fullscan_params.ks_cf, message=f"Will run command {cmd}",
                        user=self.fullscan_params.fullscan_user,
                        password=self.fullscan_params.fullscan_user_password) as scan_op_event:

            self.current_operation_stat = FullScanOperationStat(
                op_type=scan_op_event.__class__.__name__,
                nemesis_at_start=self.db_node.running_nemesis,
                cmd=cmd
            )

            with self.fullscan_params.db_cluster.cql_connection_patient(
                    node=self.db_node,
                    connect_timeout=300,
                    user=self.fullscan_params.fullscan_user,
                    password=self.fullscan_params.fullscan_user_password) as session:
                try:
                    scan_op_event.message = ''
                    start_time = time.time()
                    result = self.execute_query(session=session, cmd=cmd, event=scan_op_event)
                    if result:
                        self.fetch_result_pages(result=result, read_pages=self.fullscan_stats.read_pages)
                    if not scan_op_event.message:
                        scan_op_event.message = f"{type(self).__name__} operation ended successfully"
                except Exception as exc:  # pylint: disable=broad-except
                    self.log.error(traceback.format_exc())
                    msg = repr(exc)
                    self.current_operation_stat.exceptions.append(repr(exc))
                    msg = f"{msg} while running " \
                          f"Nemesis: {self.db_node.running_nemesis}" if self.db_node.running_nemesis else msg
                    scan_op_event.message = msg

                    if self.db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        scan_op_event.severity = Severity.WARNING
                    else:
                        scan_op_event.severity = Severity.ERROR
                finally:
                    duration = time.time() - start_time
                    self.fullscan_stats.time_elapsed += duration
                    self.fullscan_stats.scans_counter += 1
                    self.current_operation_stat.nemesis_at_end = self.db_node.running_nemesis
                    self.current_operation_stat.duration = duration
                    # success is True if there were no exceptions
                    self.current_operation_stat.success = not bool(self.current_operation_stat.exceptions)
                    self.update_stats(self.current_operation_stat)
                    return self.current_operation_stat  # pylint: disable=lost-exception

    def update_stats(self, new_stat):
        self.fullscan_stats.stats.append(new_stat)

    def run_scan_operation(self, cmd: str = None) -> FullScanOperationStat:
        return self.run_scan_event(cmd=cmd or self.randomly_form_cql_statement(), scan_event=self.scan_event)

    def fetch_result_pages(self, result, read_pages):
        self.log.debug('Will fetch up to %s result pages..', read_pages)
        pages = 0
        while result.has_more_pages and pages <= read_pages:
            if read_pages > 0:
                pages += 1


class FullScanOperation(FullscanOperationBase):
    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullScanEvent, **kwargs)

    def randomly_form_cql_statement(self) -> str:
        base_query = FullScanAggregateCommands.SELECT_ALL.base_query
        cmd = base_query.substitute(ks_cf=self.fullscan_params.ks_cf,
                                    timeout=f" USING TIMEOUT {self.fullscan_params.full_scan_operation_limit}s",
                                    bypass_cache=BYPASS_CACHE_VALUES[0])
        return cmd


class FullPartitionScanOperation(FullscanOperationBase):
    """
    Run a full scan of a partition, assuming it has a clustering key and multiple rows.
    It runs a reversed query of a partition, then optionally runs a normal partition scan in order
    to validate the reversed-query output data.

    Should support the following query options:
    1) ck < ?
    2) ck > ?
    3) ck > ? and ck < ?
    4) order by ck desc
    5) limit <int>
    6) paging
    """
    reversed_query_filter_ck_by = {'lt': ' and {} < {}', 'gt': ' and {} > {}', 'lt_and_gt': ' and {} < {} and {} > {}',
                                   'no_filter': ''}

    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullPartitionScanReversedOrderEvent, **kwargs)
        self.table_clustering_order = ""
        self.reversed_order = 'desc' if self.table_clustering_order.lower() == 'asc' else 'asc'
        self.reversed_query_filter_ck_stats = {'lt': {'count': 0, 'total_scan_duration': 0},
                                               'gt': {'count': 0, 'total_scan_duration': 0},
                                               'lt_and_gt': {'count': 0, 'total_scan_duration': 0},
                                               'no_filter': {'count': 0, 'total_scan_duration': 0}}
        self.ck_filter = ''
        self.limit = ''
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                               encoding='utf-8')

    def get_table_clustering_order(self) -> str:
        node = self._get_random_node()
        try:
            with self.fullscan_params.db_cluster.cql_connection_patient(node=node, connect_timeout=300) as session:
                # Using CL ONE. No need for a quorum since querying a constant fixed attribute of a table.
                session.default_consistency_level = ConsistencyLevel.ONE
                return get_table_clustering_order(ks_cf=self.fullscan_params.ks_cf,
                                                  ck_name=self.fullscan_params.ck_name, session=session)
        except Exception as error:  # pylint: disable=broad-except
            self.log.error(traceback.format_exc())
            self.log.error('Failed getting table %s clustering order through node %s : %s',
                           self.fullscan_params.ks_cf, node.name,
                           error)
        raise Exception('Failed getting table clustering order from all db nodes')

    def randomly_form_cql_statement(self) -> Optional[tuple[str, str]]:  # pylint: disable=too-many-branches
        """
        The purpose of this method is to form a random reversed-query out of all options, like:
            select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc

        This consists of:
        1) Select a random partition found in requested table.
        2) randomly add a CQL LIMIT and 'bypass cache' to query.
        3) Add a random CK filter with random row values.
        :return: a CQL reversed-query
        """
        db_node = self._get_random_node()

        with self.fullscan_params.db_cluster.cql_connection_patient(
                node=db_node, connect_timeout=300) as session:
            ck_random_min_value = self.generator.randint(a=1, b=self.fullscan_params.rows_count)
            ck_random_max_value = self.generator.randint(a=ck_random_min_value, b=self.fullscan_params.rows_count)
            self.ck_filter = ck_filter = self.generator.choice(list(self.reversed_query_filter_ck_by.keys()))

            if pks := get_partition_keys(ks_cf=self.fullscan_params.ks_cf, session=session, pk_name=self.fullscan_params.pk_name):
                partition_key = self.generator.choice(pks)
                # Form a random query out of all options, like:
                # select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc
                # limit 3467 bypass cache
                selected_columns = [self.fullscan_params.pk_name, self.fullscan_params.ck_name]
                if self.fullscan_params.include_data_column:
                    selected_columns.append(self.fullscan_params.data_column_name)
                reversed_query = f'select {",".join(selected_columns)} from {self.fullscan_params.ks_cf}' + \
                    f' where {self.fullscan_params.pk_name} = {partition_key}'
                query_suffix = self.limit = ''
                # Randomly add CK filtering ( less-than / greater-than / both / non-filter )

                # example: rows-count = 20, ck > 10, ck < 15, limit = 3 ==> ck_range = [11..14] = 4
                # ==> limit < ck_range
                # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                # order by ck desc limit 5
                # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                match ck_filter:
                    case 'lt_and_gt':
                        # Example: select * from scylla_bench.test where pk = 1 and ck > 10 and ck < 15 order by ck desc
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_max_value,
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                    case 'gt':
                        # example: rows-count = 20, ck > 10, limit = 5 ==> ck_range = 20 - 10 = 10 ==> limit < ck_range
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                    case 'lt':
                        # example: rows-count = 20, ck < 10, limit = 5 ==> limit < ck_random_min_value (ck_range)
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck < 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck >= 5 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                query_suffix = f"{query_suffix} {self.generator.choice(BYPASS_CACHE_VALUES)}"
                normal_query = reversed_query + query_suffix
                if random.choice([False] + [True]):  # Randomly add a LIMIT
                    self.limit = random.randint(a=1, b=self.fullscan_params.rows_count)
                    query_suffix = f' limit {self.limit}' + query_suffix
                reversed_query += f' order by {self.fullscan_params.ck_name} {self.reversed_order}' + query_suffix
                self.log.debug('Randomly formed normal query is: %s', normal_query)
                self.log.debug('[scan: %s, type: %s] Randomly formed reversed query is: %s', self.fullscan_stats.scans_counter,
                               ck_filter, reversed_query)
            else:
                self.log.debug('No partition keys found for table: %s! A reversed query cannot be executed!',
                               self.fullscan_params.ks_cf)
                return None
        return normal_query, reversed_query

    def fetch_result_pages(self, result: ResponseFuture, read_pages):
        self.log.debug('Will fetch up to %s result pages.."', read_pages)
        self.fullscan_stats.number_of_rows_read = 0
        handler = PagedResultHandler(future=result, scan_operation=self)
        handler.finished_event.wait()

        if handler.error:
            self.log.warning("Got a Page Handler error: %s", handler.error)
            raise handler.error
        self.log.debug('Fetched a total of %s pages', handler.current_read_pages)

    def execute_query(
            self, session, cmd: str,
            event: Type[FullScanEvent | FullPartitionScanEvent
                        | FullPartitionScanReversedOrderEvent]) -> ResponseFuture:
        self.log.debug('Will run command "%s"', cmd)
        session.default_fetch_size = self.fullscan_params.page_size
        session.default_consistency_level = ConsistencyLevel.ONE
        return session.execute_async(cmd)

    def reset_output_files(self):
        self.normal_query_output.close()
        self.reversed_query_output.close()
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                               encoding='utf-8')

    def _compare_output_files(self) -> bool:
        self.normal_query_output.flush()
        self.reversed_query_output.flush()
        result = False

        with tempfile.NamedTemporaryFile(mode='w+', delete=True, encoding='utf-8') as reorder_normal_query_output:
            cmd = f'tac {self.normal_query_output.name}'

            if self.limit:
                cmd += f' | head -n {self.limit}'

            cmd += f' > {reorder_normal_query_output.name}'
            LOCAL_CMD_RUNNER.run(cmd=cmd, ignore_status=True)
            reorder_normal_query_output.flush()
            LOCAL_CMD_RUNNER.run(cmd=f'cp {reorder_normal_query_output.name} {self.normal_query_output.name}',
                                 ignore_status=True)
            self.normal_query_output.flush()
            file_names = f' {self.normal_query_output.name} {self.reversed_query_output.name}'
            diff_cmd = f"diff -y --suppress-common-lines {file_names}"
            self.log.debug("Comparing scan queries output files by: %s", diff_cmd)
            result = LOCAL_CMD_RUNNER.run(cmd=diff_cmd, ignore_status=True)

        if not result.stderr:
            if not result.stdout:
                self.log.debug("Compared output of normal and reversed queries is identical!")
                result = True
            else:
                self.log.warning("Normal and reversed queries output differs: \n%s", result.stdout.strip())
                ls_cmd = f"ls -alh {file_names}"
                head_cmd = f"head {file_names}"
                for cmd in [ls_cmd, head_cmd]:
                    result = LOCAL_CMD_RUNNER.run(cmd=cmd, ignore_status=True)
                    if result.stdout:
                        stdout = result.stdout.strip()
                        self.log.debug("%s command output is: \n%s", cmd, stdout)
        else:
            self.log.warning("Comparison of output of normal and reversed queries failed with stderr:\n%s",
                             result.stderr)

        self.reset_output_files()
        return result

    def run_scan_operation(self, cmd: str = None):  # pylint: disable=too-many-locals
        self.table_clustering_order = self.get_table_clustering_order()
        queries = self.randomly_form_cql_statement()

        if not queries:
            return

        normal_query, reversed_query = queries

        full_partition_op_stat = FullScanOperationStat(
            op_type=self.__class__.__name__,
            nemesis_at_start=self.db_node.running_nemesis,
            cmd=str(queries)
        )

        reversed_op_stat = self.run_scan_event(cmd=reversed_query, scan_event=FullPartitionScanReversedOrderEvent)
        self.reversed_query_filter_ck_stats[self.ck_filter]['count'] += 1
        self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] += self.fullscan_stats.time_elapsed
        count = self.reversed_query_filter_ck_stats[self.ck_filter]['count']
        average = self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] / count
        self.log.debug('Average %s scans duration of %s executions is: %s', self.ck_filter, count, average)

        if self.fullscan_params.validate_data:
            self.log.debug('Executing the normal query: %s', normal_query)
            self.scan_event = FullPartitionScanEvent
            regular_op_stat = self.run_scan_event(cmd=normal_query, scan_event=self.scan_event)
            comparison_result = self._compare_output_files()
            full_partition_op_stat.nemesis_at_end = self.db_node.running_nemesis
            full_partition_op_stat.exceptions.append(regular_op_stat.exceptions)
            full_partition_op_stat.exceptions.append(reversed_op_stat.exceptions)
            if comparison_result and not full_partition_op_stat.exceptions:
                full_partition_op_stat.success = True
            else:
                full_partition_op_stat.success = False


class FullScanAggregatesOperation(FullscanOperationBase):
    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullScanAggregateEvent, **kwargs)
        self._session_execution_timeout = self.fullscan_params.full_scan_aggregates_operation_limit + 10*60

    def randomly_form_cql_statement(self) -> str:
        cmd = FullScanAggregateCommands.AGG_COUNT_ALL.base_query.substitute(
            ks_cf=self.fullscan_params.ks_cf,
            timeout=f" USING TIMEOUT {self.fullscan_params.full_scan_aggregates_operation_limit}s",
            bypass_cache=BYPASS_CACHE_VALUES[0]
        )
        return cmd

    def execute_query(self, session, cmd: str,
                      event: Type[FullScanEvent | FullPartitionScanEvent
                                  | FullPartitionScanReversedOrderEvent]) -> None:
        self.log.debug('Will run command %s', cmd)
        try:
            cmd_result = session.execute(
                query=cmd, trace=True, timeout=self._session_execution_timeout)
        except OperationTimedOut as exc:
            self.log.error(traceback.format_exc())
            self.current_operation_stat.exceptions.append(repr(exc))
            event.message = f"{type(self).__name__} operation failed: {repr(exc)}," \
                            f"session execution timeout {self._session_execution_timeout} is exceeded "
            event.severity = Severity.ERROR
            return
        except ReadTimeout as exc:
            self.current_operation_stat.exceptions.append(repr(exc))
            self.current_operation_stat.exceptions.append(repr(exc))
            if "Operation timed out" in repr(exc):
                event.message = f"{type(self).__name__} operation failed due to operation timed out: {repr(exc)}," \
                                f" full_scan_aggregates_operation_limit=" \
                                f"{self.fullscan_params.full_scan_aggregates_operation_limit}"
            else:
                event.message = f"{type(self).__name__} operation failed, ReadTimeout error: {repr(exc)}"
            event.severity = Severity.ERROR
            return

        message, severity = self._validate_fullscan_result(cmd_result)
        if not severity:
            event.message = f"{type(self).__name__} operation ended successfully: {message}"
        else:
            event.severity = severity
            event.message = f"{type(self).__name__} operation failed: {message}"
    def _validate_fullscan_result(
            self, cmd_result: ResultSet):
        regex_found = False
        dispatch_forward_statement_regex_pattern = re.compile(r"Dispatching forward_request to \d* endpoints")
        result = cmd_result.all()

        if not result:
            message = "Fullscan failed - got empty result"
            self.log.warning(message)
            return message, Severity.ERROR
        output = "\n".join([str(i) for i in result])
        if int(result[0].count) <= 0:
            return f"Fullscan failed - count is not bigger than 0: {output}", Severity.ERROR
        get_query_trace = cmd_result.get_query_trace().events
        for trace_event in get_query_trace:
            if dispatch_forward_statement_regex_pattern.search(str(trace_event)):
                regex_found = True
                break
        self.log.debug("Fullscan aggregation result: %s", output)

        if not regex_found:
            self.log.debug("\n".join([str(i) for i in get_query_trace]))
            self.log.debug("Fullscan aggregation result: %s", output)
            return "Fullscan failed - 'Dispatching forward_request' message was not found in query trace events", \
                Severity.WARNING

        return f'result {result[0]}', None


class PagedResultHandler:

    def __init__(self, future: ResponseFuture, scan_operation: FullPartitionScanOperation):
        self.error = None
        self.finished_event = threading.Event()
        self.future = future
        self.max_read_pages = scan_operation.fullscan_stats.read_pages
        self.current_read_pages = 0
        self.log = logging.getLogger(self.__class__.__name__)
        self.scan_operation = scan_operation
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def _row_to_string(self, row, include_data_column: bool = False) -> str:
        row_string = str(getattr(row, self.scan_operation.fullscan_params.pk_name))
        row_string += str(getattr(row, self.scan_operation.fullscan_params.ck_name))
        if include_data_column:
            row_string += str(getattr(row, self.scan_operation.fullscan_params.data_column_name))
        return f'{row_string}\n'

    def handle_page(self, rows):
        include_data_column = self.scan_operation.fullscan_params.include_data_column
        if self.scan_operation.scan_event == FullPartitionScanEvent:
            for row in rows:
                self.scan_operation.normal_query_output.write(
                    self._row_to_string(row=row, include_data_column=include_data_column))
        elif self.scan_operation.scan_event == FullPartitionScanReversedOrderEvent:
            self.scan_operation.fullscan_stats.number_of_rows_read += len(rows)
            if self.scan_operation.fullscan_params.validate_data:
                for row in rows:
                    self.scan_operation.reversed_query_output.write(
                        self._row_to_string(row=row, include_data_column=include_data_column))

        if self.future.has_more_pages and self.current_read_pages <= self.max_read_pages:
            self.log.debug('Will fetch the next page: %s', self.current_read_pages)
            self.future.start_fetching_next_page()
            if self.max_read_pages > 0:
                self.current_read_pages += 1
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()
