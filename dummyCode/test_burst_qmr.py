# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import threading
import logging
from plumbum import ProcessExecutionError
import pytest
from contextlib import contextmanager
from time import sleep

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import get_burst_cluster_arn
from raff.burst.burst_test import get_burst_cluster_name
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_test import verify_query_bursted
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.base_test import run_priviledged_query
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.profile import Profiles
from raff.common.region import DEFAULT_REGION
from raff.common.ssh_user import SSHUser
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "verify_query_bursted"]

BASIC_WLM_CONFIGS = '''
[
    {
        "query_group": ["burst"],
        "concurrency_scaling": "auto",
        "query_concurrency": 2
    },
    {
        "query_group": ["burst_next"],
        "user_group": ["*"],
        "user_group_wild_card":1,
        "concurrency_scaling": "auto",
        "query_concurrency": 2
    }
]
'''

COMMON_QMR_WLM_CONFIGS = '''
[
    $more_class$
    {
        "query_group": ["burst"],
        "concurrency_scaling": "auto",
        "query_concurrency": 2,
        "rules":
        [
            {
                "rule_name": "$rule_name$",
                "action": "$action$",
                "predicate":
                [
                    {
                        "metric_name": "$metric_name$",
                        "operator": ">",
                        "value": $predicate_value$
                    }
                ]
            }
        ]
    },
    {
        "query_group": ["burst_next"],
        "user_group": ["*"],
        "user_group_wild_card":1,
        "concurrency_scaling": "auto",
        "query_concurrency": 2
    }
]
'''

QMR_WLM_CONFIGS = COMMON_QMR_WLM_CONFIGS.replace("$more_class$", "")

CUSTOM_QMR_GUCS = {
    'enable_query_monitoring_rules': 'true',
    'enable_burst_failure_handling': 'false',
    'wlm_rules_eval_frequency': 1,
    'metrics_capture_frequency': 1,
    'burst_qmr_fetch_violators_s': 1,
    'wlm_json_configuration': ''.join(BASIC_WLM_CONFIGS.split('\n'))
}

LONG_JOIN = "web_sales A, web_sales B, web_sales C"


class TestBurstQmr(BurstTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(params=[
            # rule_name, action, metric_name, predicate_value, query,
            # should_abort, event_to_be_set_on_burst
            ("my_rule1", "abort", "nested_loop_join_row_count", '200',
             "select count(*) from {};".format(LONG_JOIN),
             True, None),
            ("my_rule2", "abort", "join_row_count", '200',
             "select count(*) from {};".format(LONG_JOIN),
             True, None),
            # If the rule is not changed, then the following query will abort
            # with my_rule2. However, if the rule is change with my_rule3, the
            # following query will keep executing. This is a proof that the
            # wlm qmr rules are changing correctly as expected.
            ("my_rule3", "abort", "return_row_count", '2',
             # Following query should not abort if the rule was set
             # correctly on the burst side.
             "select count(*) from {};".format(LONG_JOIN),
             # should not abort
             False, None),
            ("my_rule4", "abort", "return_row_count", '2',
             "select * from {};".format(LONG_JOIN),
             True, None),
            ("my_rule5", "abort", "scan_row_count", '1',
             "select count(*) from {};".format(LONG_JOIN),
             True,
             # Slow down to catch the spectrum row counts
             "EtTestBurstQmrRulesSlowQuery,EtSkipStatOptimization"),
            ("my_rule6", "abort", "query_execution_time", '10',
             "select count(*) from {};".format(LONG_JOIN),
             True, None),
            ("my_rule7", "abort", "query_blocks_read", '1',
             "select count(*) from {};".format(LONG_JOIN),
             True,
             # Slow down to catch the spectrum block reads
             "EtTestBurstQmrRulesSlowQuery"),

            # Following are tests for hop action.
            ("my_rule8", "hop", "nested_loop_join_row_count", '200',
             "select count(*) from {};".format(LONG_JOIN),
             False, None),
            ("my_rule9", "hop", "scan_row_count", '1',
             "select count(*) from {};".format(LONG_JOIN),
             False,
             # Slow down to catch the spectrum row counts
             "EtTestBurstQmrRulesSlowQuery,EtSkipStatOptimization"),
            ("my_rule10", "hop", "query_execution_time", '10',
             "select count(*) from {};".format(LONG_JOIN),
             False, None),
        ]))

    @contextmanager
    def change_wlm_and_exec(self, cluster, sc, conf, reset_conf):
        """
        Context manager to change the wlm configuration and reset upon exit.
        :param cluster: raff cluster.
        :param sc: service class for which to check application of the qmr.
        :param conf: changed configuration to apply.
        :param reset_conf: reset configuration.
        :return: None
        """
        # Apply the new config first.
        cluster.set_padb_conf_value_unsafe(
            key='wlm_json_configuration', value=conf, restart=False)
        cluster.run_xpx('update_wlm_json_config_inmemory')
        # Give the command some time to be applied.
        self.wait_wlm_apply(sc, should_have_qmr=True)
        yield
        # Reset before leaving.
        cluster.set_padb_conf_value_unsafe(
            key='wlm_json_configuration', value=reset_conf, restart=False)
        cluster.run_xpx('update_wlm_json_config_inmemory')
        # Give the command some time to be applied.
        self.wait_wlm_apply(sc, should_have_qmr=False)

    def wait_wlm_apply(self, sc, should_have_qmr):
        with self.db.cursor() as cursor_db:
            cursor_db.execute("set query_group to metrics;")
            trial = 30
            while trial > 0:
                sleep(1)
                cursor_db.execute(
                    "select count(*) from "
                    "stv_wlm_qmr_config where service_class = {}".format(sc))
                qmr_config_count = int(cursor_db.fetch_scalar())
                if should_have_qmr and qmr_config_count != 0:
                    return True
                if not should_have_qmr and qmr_config_count == 0:
                    return True
                trial -= 1
        raise Exception("WLM config was not applied")

    def run_long_query(self, db_session, querytxt):
        try:
            with db_session.cursor() as cursor:
                cursor.execute(
                    "set query_group to burst; " + querytxt
                )
        except Exception as e:
            log.info("Expcted to be killed: " + str(e))

    def kill_long_running_queries(self):
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and "
                "(B.service_class = 6 or B.service_class = 7) and "
                "A.label = 'burst'")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))

    @contextmanager
    def long_running_context(self, cluster, querytxt):
        try:
            thread = threading.Thread(
                target=self.run_long_query,
                args=(self.get_db_session(cluster), querytxt))
            thread.start()
            yield thread
        finally:
            self.kill_long_running_queries()

    def get_db_session(self, cluster):
        db_session = DbSession(cluster.get_conn_params())
        with db_session.cursor() as cursor, self.db.cursor() as cursor_db:
            # Get regular user name.
            user = db_session.session_ctx.username
            # Get the user id of the regular user.
            cursor_db.execute(
                "select usesysid from pg_user "
                "where usename = '{}'".format(user)
            )
            userid = cursor_db.fetch_scalar()
            log.info("Regular user id: {}".format(userid))
            cursor_db.execute(
                "GRANT ALL ON ALL TABLES IN SCHEMA public "
                "to {}".format(user)
            )
        return db_session

    def is_burst_query_running(self, db_session, ts):
        with db_session.cursor() as cursor:
            is_burst_running = 0
            trial = 30
            while is_burst_running == 0 and trial > 0:
                query = ("select count(*) from stv_inflight "
                         "where concurrency_scaling_status = 1 and "
                         "EXTRACT(epoch FROM starttime) >= {}")
                cursor.execute("set query_group to metrics;")
                cursor.execute(query.format(ts))
                is_burst_running = cursor.fetch_scalar()
                trial -= 1
                sleep(1)
            if is_burst_running > 0:
                return True
            else:
                return False

    def is_action_taken_on_burst_query(self, ts, rule_name, action):
        with self.db.cursor() as cursor:
            query = ("select query "
                     "from stl_wlm_rule_action "
                     "where rule = '{}' and "
                     "action = '{}' and "
                     "EXTRACT(epoch FROM recordtime) >= {}")
            cursor.execute("set query_group to metrics;")
            cursor.execute(query.format(rule_name, action, ts))
            if cursor.rowcount == 1:
                return True
            else:
                return False

    def log_info_from_burst_cluster(self, burst_cluster, action, ts):
        client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                region=DEFAULT_REGION)
        burst_cluster_client = client.describe_cluster(burst_cluster)
        with burst_cluster_client.get_leader_ssh_conn() as ssh_conn:
            cmd = "grep 'Consolidated WLM Monitoring Query' " \
                  "/rdsdbdata/data/log/start_node.log"
            _, stdout, _ = ssh_conn.run_remote_cmd(cmd, user=SSHUser.RDSDB)
            log.info(stdout)
        with self.db.cursor() as cursor:
            query = ("select query, btrim(rule), btrim(action), recordtime "
                     "from stl_wlm_rule_action "
                     "where action = '{}' and "
                     "EXTRACT(epoch FROM recordtime) >= {}")
            cursor.execute("set query_group to metrics;")
            cursor.execute(query.format(action, ts))
            rows = cursor.fetchall()
            for row in rows:
                log.info(row)

    def get_qmr_change_count_from_burst_cluster(self, burst_cluster,
                                                rule_name):
        client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                region=DEFAULT_REGION)
        burst_cluster_client = client.describe_cluster(burst_cluster)
        with burst_cluster_client.get_leader_ssh_conn() as ssh_conn:
            cmd = "grep -c 'Consolidated WLM Monitoring Query.*{}' " \
                  "/rdsdbdata/data/log/start_node.log".format(rule_name)
            try:
                _, stdout, _ = ssh_conn.run_remote_cmd(cmd, user=SSHUser.RDSDB)
                log.info("Burst cluster qmr change count for {}: {}.".format(
                    rule_name, stdout))
                return int(stdout)
            except ProcessExecutionError:
                return 0

    def wait_for_more_qmr_change_on_burst_cluster(self, burst_cluster,
                                                  rule_name, initial_count):
        trial = 30
        while trial > 0:
            sleep(1)
            current_count = self.get_qmr_change_count_from_burst_cluster(
                burst_cluster, rule_name)
            if current_count > initial_count:
                return
            trial -= 1
        raise Exception("Qmr config did not change on burst cluster.")

    def burst_qmr_rules(self, cluster, db_session, rule_name, action,
                        querytxt, should_abort, burst_cluster):
        with db_session.cursor() as cursor:
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()

        with self.long_running_context(cluster, querytxt) as thread:
            if not self.is_burst_query_running(self.db, ts):
                pytest.fail("Query didn't burst.")

            thread.join(30)
            # Let's wait now ~300sec to make sure hop/abort action is taken.
            # is_burst_query_running checks if the stv_inflight has the query
            # marked bursting. This does not ensure that the query is actually
            # executing on burst cluster. Indeed, version switching can take
            # more than a minute, and this test will fail. So, let's check
            # every second for 300 times, if the query was aborted or hopped.
            for i in range(300):
                sleep(1)
                if action == "hop":
                    if self.is_action_taken_on_burst_query(
                            ts, rule_name, "hop(restart)"):
                        return
                elif action == "abort":
                    if should_abort:
                        if self.is_action_taken_on_burst_query(
                                ts, rule_name, "abort"):
                            return
                    else:
                        assert self.is_burst_query_running(self.db, ts), \
                            "Query should not be aborted"
                        return

            if action == "hop":
                self.log_info_from_burst_cluster(burst_cluster, action, ts)
                pytest.fail("Burst query should have hopped.")
            elif action == "abort":
                self.log_info_from_burst_cluster(burst_cluster, action, ts)
                pytest.fail("Burst query was not aborted")


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=dict({'burst_mode': 3}, **CUSTOM_QMR_GUCS))
@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.serial_only
class TestBurstQmrClusterOnly(TestBurstQmr):
    """
    Test end-to-end QMR rules in action.
    """
    @contextmanager
    def setup_burst_event(self, burst_cluster, events=None):
        """
        Setup the burst cluster with events when needed.
        """
        if not events:
            events = []
        client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                region=DEFAULT_REGION)
        burst_cluster_client = client.describe_cluster(burst_cluster)
        for event in events:
            burst_cluster_client.set_event(event)
        yield
        for event in events:
            burst_cluster_client.unset_event(event)

    @pytest.mark.no_jdbc
    def test_burst_qmr(self, cluster, db_session, vector):
        """
        Test QMR aborts queries on burst clusters when violating rules.
        """
        rule_name = vector.params[0]
        action = vector.params[1]
        metric_name = vector.params[2]
        predicate_value = vector.params[3]
        querytxt = vector.params[4]
        should_abort = vector.params[5]
        burst_events = [e.strip() for e in vector.params[6].split(',')] \
            if vector.params[6] else []

        burst_cluster = get_burst_cluster_name(cluster)
        if burst_cluster is None:
            raise ValueError("No burst clusters acquired")

        # In case of version switching first connection to a burst cluster
        # can take a long time to establish, which will cause the test to
        # fail. To avoid this, ping the burst cluster here.
        burst_cluster_arn = get_burst_cluster_arn(cluster)
        results = cluster.run_xpx("burst_ping {}".format(burst_cluster_arn))
        line = ''
        for result_line in results:
            result_line = str(result_line)
            if result_line.startswith('INFO'):
                line = result_line
                break
        assert line.startswith('INFO'), \
            "Connection to burst cluster is unsuccessful."
        result = line[line.find("<") + 1:line.find(">")]
        assert result.startswith('Ping Result'), \
            "Connection to burst cluster is unsuccessful."

        initial_qmr_change_count = \
            self.get_qmr_change_count_from_burst_cluster(burst_cluster,
                                                         rule_name)
        config = ''.join(QMR_WLM_CONFIGS.split('\n')). \
            replace('$rule_name$', rule_name). \
            replace('$action$', action). \
            replace('$metric_name$', metric_name). \
            replace('$predicate_value$', predicate_value)
        reset_conf = ''.join(BASIC_WLM_CONFIGS.split('\n'))
        with self.change_wlm_and_exec(
                cluster, sc=6, conf=config, reset_conf=reset_conf), \
                self.setup_burst_event(burst_cluster, burst_events):
            self.wait_for_more_qmr_change_on_burst_cluster(
                burst_cluster, rule_name, initial_qmr_change_count)
            self.burst_qmr_rules(cluster, db_session, rule_name, action,
                                 querytxt, should_abort, burst_cluster)

    def burst_worker(self, db_session):
        self.execute_test_file('burst_query', session=db_session,
                               query_group='burst')

    @pytest.mark.no_jdbc
    def test_burst_slow_qmr_send(self, cluster, db_session,
                                 verify_query_bursted):
        """
        Test successful query execution during slow QMR send to burst.
        """
        burst_cluster = get_burst_cluster_name(cluster)
        if burst_cluster is None:
            raise ValueError("No burst clusters acquired")

        rule_name = 'my_rule_slow_qmr'
        action = 'abort'
        config = ''.join(QMR_WLM_CONFIGS.split('\n')). \
            replace('$rule_name$', rule_name). \
            replace('$action$', action). \
            replace('$metric_name$', 'nested_loop_join_row_count'). \
            replace('$predicate_value$', '200')
        reset_conf = ''.join(BASIC_WLM_CONFIGS.split('\n'))

        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics")
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()

        with cluster.event("EtSimulateSlowBurstQmrRuleSending", "sleep=100"), \
                cluster.event("EtBurstTracing", "level=ElDebug5"), \
                self.change_wlm_and_exec(cluster, sc=6, conf=config,
                                         reset_conf=reset_conf):
            trial = 30
            qmr_rule_send_count = 0
            query = """
                    SET query_group to metrics;
                    SELECT eventtime FROM stl_event_trace
                    where EXTRACT(EPOCH FROM eventtime) >= '{}'
                    and message like '%BurstManager::SendQmrRulesToBurst%'
                    """.format(ts)
            while qmr_rule_send_count == 0 and trial > 0:
                with self.db.cursor() as cursor_bstrap:
                    rows = run_priviledged_query(cluster, cursor_bstrap, query)
                    qmr_rule_send_count = len(rows)
                    sleep(1)
                    trial -= 1
                if trial <= 0:
                    pytest.fail("Qmr rule send to burst never started")
            # If the rule is sent to burst the following query will
            # abort as it has nested_loop_join_row_count > 200 and the test
            # will fail. If it doesn't abort then, the qmr rule sending is
            # sleeping and will keep checking that the thread is running.
            querytxt = "select count(*) from {};".format(LONG_JOIN)
            with self.long_running_context(cluster, querytxt) as thread:
                if not self.is_burst_query_running(self.db, ts):
                    pytest.fail("Query didn't burst.")
                thread.join(30)
                # Let's wait now ~30sec to make sure no abort action is taken.
                for i in range(30):
                    if self.is_action_taken_on_burst_query(
                            ts, rule_name, "abort"):
                        pytest.fail("Burst query should not be aborted")
                # If the lock was kept during the qmr rule send, following
                # query would hang.
                burst_thread = threading.Thread(
                    target=self.burst_worker,
                    args=(self.get_db_session(cluster),))
                burst_thread.daemon = True
                burst_thread.start()
                # Burst thread should finish soon. If it hangs for 100 sec
                # let's quit, verify_query_bursted will fail the test.
                burst_thread.join(100)


QMR_WLM_CONFIGS_WITH_BURST_IN_SC_7 = ''.join(
    COMMON_QMR_WLM_CONFIGS.split('\n')).replace(
    "$more_class$", ''.join('''{
        "query_group": ["burst_other"],
        "user_group": ["*"],
        "user_group_wild_card":1,
        "concurrency_scaling": "auto",
        "query_concurrency": 2
    },'''.split('\n'))).replace('$rule_name$', 'my_rule_burst_sc_7'). \
    replace('$action$', 'abort'). \
    replace('$metric_name$', 'nested_loop_join_row_count'). \
    replace('$predicate_value$', '200')


CUSTOM_QMR_GUCS_FOR_BURST_IN_SC_7 = {
    'enable_query_monitoring_rules': 'true',
    'enable_burst_failure_handling': 'false',
    'wlm_rules_eval_frequency': 1,
    'metrics_capture_frequency': 1,
    'burst_qmr_fetch_violators_s': 1,
    'wlm_json_configuration': ''.join(
        QMR_WLM_CONFIGS_WITH_BURST_IN_SC_7.split('\n'))
}


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=dict({'burst_mode': 3},
                                         **CUSTOM_QMR_GUCS_FOR_BURST_IN_SC_7))
@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.serial_only
class TestBurstQmrInServiceClass7(TestBurstQmr):
    @pytest.mark.no_jdbc
    def test_burst_qmr_in_service_class_7(
            self, cluster, db_session, verify_query_bursted):
        """
        Test successful burst qmr in when the query burst in sc 7.
        """
        burst_cluster = get_burst_cluster_name(cluster)
        if burst_cluster is None:
            raise ValueError("No burst clusters acquired")

        rule_name = 'my_rule_burst_sc_7'
        action = 'abort'
        querytxt = "select count(*) from {};".format(LONG_JOIN)
        self.burst_qmr_rules(cluster, db_session, rule_name, action,
                             querytxt, True, burst_cluster)

        stl_query = "select count(*) from stl_query_metrics where " \
                    "service_class_name ilike '%queue 7%';"
        client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                region=DEFAULT_REGION)
        burst_client = client.describe_cluster(burst_cluster)
        trial = 60
        query_count = 0
        # Run the query many times to make sure wlm hasn't crashed as it
        # had in RedshiftCP-7646.
        while trial > 0 and query_count == 0:
            sleep(1)
            query_count = int(run_bootstrap_sql(burst_client, stl_query)[0][0])
            trial -= 1
        assert query_count > 0, \
            "There should be entries for bursting from main queue 7"
