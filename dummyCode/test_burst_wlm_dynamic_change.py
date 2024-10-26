# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
from time import sleep
from threading import Thread

from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.base_test import run_priviledged_query
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

wlm_config_initial = ('''
            [
                {
                    "query_group":["q1"],
                    "auto_wlm":true,
                    "queue_type":"auto"
                },
                {
                    "query_group":["q2"],
                    "priority":"Low",
                    "queue_type":"auto",
                    "concurrency_scaling": "auto",
                    "rules":[
                        {
                            "rule_name":"change_priority_rule",
                            "action":"change_query_priority",
                            "value":"High",
                            "predicate":[{
                                "metric_name":"query_execution_time",
                                "operator":">",
                                "value":1}]
                        }]
                },
                {
                    "user_group":["u1"],
                    "priority":"Highest",
                    "queue_type":"auto",
                    "rules":[
                        {
                            "rule_name":"log_cpu_time_rule",
                            "predicate":[{
                                "metric_name":"query_cpu_time",
                                "operator":">",
                                "value":1}],
                            "action":"log"
                        }]
                }
            ]
            ''').replace('\n', '').replace(' ', '')

wlm_config_changed_qmr = ('''
            [
                {
                    "query_group":["q1"],
                    "auto_wlm":true,
                    "rules":[{
                        "rule_name" : "spill_rule",
                        "predicate" : [ {
                            "metric_name" : "query_temp_blocks_to_disk",
                            "operator" : ">",
                            "value" : 100000
                        }, {
                            "metric_name" : "query_priority",
                            "operator" : "=",
                            "value" : "low"
                        } ],
                        "action" : "change_query_priority",
                        "value" : "low"
                    } ],
                    "queue_type":"auto"
                },
                {
                    "query_group":["q2"],
                    "priority":"Low",
                    "queue_type":"auto",
                    "concurrency_scaling": "auto",
                    "rules" : [ {
                        "rule_name" : "query_abort_rule",
                        "predicate" : [ {
                            "metric_name" : "query_execution_time",
                            "operator" : ">",
                            "value" : 120
                        }],
                        "action" : "abort"
                    }]
                },
                {
                    "user_group":["u1"],
                    "rules" : [ {
                        "rule_name" : "nestloop_rule",
                        "predicate" : [ {
                            "metric_name" : "nested_loop_join_row_count",
                            "operator" : ">",
                            "value" : 100}],
                        "action":"log"
                    }],
                    "priority":"Highest",
                    "queue_type":"auto"
                }
            ]
            ''').replace('\n', '').replace(' ', '')

wlm_config_eight_queues = ('''
[
    {
        "query_group":["q4","q5"],
        "priority":"highest",
        "queue_type":"auto"
    },
    {
        "query_group":["q3"],
        "priority":"high",
        "queue_type":"auto",
        "concurrency_scaling":"auto"
    },
    {
        "query_group":["q2","q7"],
        "priority":"high",
        "queue_type":"auto",
        "rules":[
        {
            "rule_name":"priority_rule_v2",
            "action":"change_query_priority",
            "value":"High",
            "predicate":[{
                "metric_name":"query_execution_time",
                "operator":">",
                "value":1}]
        }]
    },
    {
        "query_group":["q1"],
        "user_group":[],
        "priority":"normal",
        "queue_type":"auto",
        "concurrency_scaling":"auto",
        "query_group_wild_card":1
    },
    {
        "user_group":["u3"],
        "query_group":[],
        "priority":"normal",
        "queue_type":"auto",
        "user_group_wild_card":1
    },
    {
        "user_group":["u2"],
        "query_group":["q1"],
        "priority":"low",
        "query_group_wild_card":1,
        "queue_type":"auto"
    },
    {
        "user_group":["u1"],
        "priority":"low",
        "queue_type":"auto",
        "user_group_wild_card":1
    },
    {
        "query_group":[],
        "priority":"lowest",
        "queue_type":"auto",
        "auto_wlm":true
    }

]
''').replace('\n', '').replace(' ', '')

wlm_config_four_queues = ('''
[
    {
        "query_group":["q4"],
        "priority":"highest",
        "queue_type":"auto",
        "query_group_wild_card":1
    },
    {
        "user_group":["u3"],
        "priority":"normal",
        "queue_type":"auto",
        "concurrency_scaling":"auto"
    },
    {
        "user_group":["u1","u2"],
        "auto_wlm":"true",
        "priority":"low",
        "queue_type":"auto",
        "query_group_wild_card":1
    },
    {
        "query_group":[ ],
        "priority":"high",
        "queue_type":"auto",
        "auto_wlm":true
    }
]
''').replace('\n', '').replace(' ', '')

wlm_config_one_queue = '[{"auto_wlm":true}]'

CUSTOM_PRIORITY_GUCS = {
    'autowlm_concurrency': 'true',
    'enable_query_priority': 'true',
    'enable_short_query_bias': 'false',
    'enable_result_cache': 'false',
    'metrics_capture_frequency': 1,
    'wlm_rules_eval_frequency': 1,
    'enable_burst_status_always': 'true',
    'enable_burst': 'true',
    'try_burst_first': 'true',
    'wlm_json_configuration': wlm_config_initial,
    'enable_burst_async_acquire': 'false'
}


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_PRIORITY_GUCS)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstWLMDynamicQueueAdd(BurstTest):
    def generate_sql_res_files(self):
        return True

    def run_long_heavy_query(self, db_session, priority):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to {};".format(priority))
            cursor.execute("select count(*) from web_sales A, "
                           "web_sales B, web_sales C;")

    def set_wlm_config_guc_and_reload(self, cluster, json_config):
        cluster.set_padb_conf_value_unsafe(
            key='wlm_json_configuration', value=json_config, restart=False)
        # This makes PADB re-load the wlm_json_config string from padb.conf
        # and try to apply the new configuration dynamically.
        cluster.run_xpx('update_wlm_json_config_inmemory')

    def test_burst_dynamic_queue_add_remove(self, cluster, db_session):
        """
        Test that dynamically adding/removing queues works as a dynamic change.
        It starts a long running query that bursts, then checks that the query
        survives all the changes and can be cancelled successfully.
        """
        session_ctx = SessionContext(user_type='super', schema='myschema')
        super_session = DbSession(
            cluster.get_conn_params(), session_ctx=session_ctx)

        session_ctx = SessionContext(user_type='regular')
        regular_session = DbSession(
            cluster.get_conn_params(), session_ctx=session_ctx)

        # Check initial configuration.
        self.execute_test_file(
            "wlm_dynamic_add_queue_initial_config", session=super_session)

        # Run a query with query group "q2". In initial config, this runs
        # at priority Low.
        thread = Thread(
            target=self.run_long_heavy_query, args=[regular_session, 'q2'])

        thread.start()
        sleep(10)

        # Dynamically change config: use a config with different QMR rules.
        self.set_wlm_config_guc_and_reload(cluster, wlm_config_changed_qmr)
        self.execute_test_file(
            "wlm_dynamic_add_queue_changed_qmr_config", session=super_session)

        # Dynamically change config: use a config with 8 queues.
        self.set_wlm_config_guc_and_reload(cluster, wlm_config_eight_queues)
        self.execute_test_file(
            "wlm_dynamic_add_queue_eight_queues_config", session=super_session)

        # Dynamically change config: use a config with 4 queues.
        self.set_wlm_config_guc_and_reload(cluster, wlm_config_four_queues)
        self.execute_test_file(
            "wlm_dynamic_add_queue_four_queues_config", session=super_session)

        # Dynamically change config: use a config with 1 queue.
        self.set_wlm_config_guc_and_reload(cluster, wlm_config_one_queue)
        self.execute_test_file(
            "wlm_dynamic_add_queue_one_queue_config", session=super_session)

        # Cancel the long running bursting query now, and check final status.
        rows = run_priviledged_query(
            cluster, self.db.cursor(), "set query_group to metrics; select "
            "query,pid, "
            "concurrency_scaling_status from stv_inflight "
            "where "
            "label = 'q2'")
        assert len(rows) == 1
        query = int(rows[0][0])
        pid = int(rows[0][1])
        burst_status = int(rows[0][2])
        log.info("Found query=%d ran by pid=%d" % (query, pid))
        # Check that the query is actually bursting.
        assert burst_status == 1

        # Cancel the long running bursting query now.
        run_priviledged_query(
            cluster, self.db.cursor(),
            'set query_group to metrics; select pg_cancel_backend(%d)' % (pid))

        # Give a few seconds for the query to clean up.
        sleep(10)

        # Check that query finished cleanly in SC 101.
        rows = run_priviledged_query(
            cluster, self.db.cursor(), "set query_group to metrics; select "
            "service_class, final_state from stl_wlm_query "
            "where query=%d" % (query))
        assert len(rows) == 1
        service_class = int(rows[0][0])
        final_wlm_state = rows[0][1]
        assert service_class == 101
        assert final_wlm_state.strip() == 'Completed'

        # Dynamically change config: Reset to the initial config.
        self.set_wlm_config_guc_and_reload(cluster, wlm_config_initial)
        self.execute_test_file(
            "wlm_dynamic_add_queue_initial_config", session=super_session)
