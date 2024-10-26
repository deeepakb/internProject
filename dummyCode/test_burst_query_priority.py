# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
from time import sleep

from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.base_test import run_priviledged_query
from contextlib import contextmanager

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [
    setup_teardown_burst
]


BASIC_WLM_CONFIGS = '''
[
    {
        "query_group": ["burst"],
        "concurrency_scaling": "auto",
        "priority": "high",
        "queue_type": "auto"
    },
    {
        "query_group": ["noburst"],
        "concurrency_scaling": "off",
        "priority": "low",
        "queue_type": "auto"
    },
    {
        "auto_wlm": true
    }
]
'''


BASIC_MANUAL_WLM_CONFIGS = '''
[
    {
        "query_group": ["burst"],
        "concurrency_scaling": "auto",
        "query_concurrency": 1
    },
    {
        "query_group": ["noburst"],
        "concurrency_scaling": "off",
        "query_concurrency": 2
    }
]
'''

CUSTOM_QMR_GUCS = {
    'autowlm_concurrency': 'true',
    'enable_query_priority': 'true',
    'enable_burst_status_always': 'true',
    'enable_burst': 'true',
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'wlm_json_configuration': ''.join(BASIC_WLM_CONFIGS.split('\n'))
}

CUSTOM_MANUAL_GUCS = {
    'autowlm_concurrency': 'true',
    'enable_query_priority': 'true',
    'enable_burst_status_always': 'true',
    'enable_burst': 'true',
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'wlm_json_configuration': ''.join(BASIC_MANUAL_WLM_CONFIGS.split('\n'))
}

@contextmanager
def set_event_priority(cluster):
    cluster.set_event("EtChangeQueryPriorityDebugInfo,level=ElDebug2")
    yield
    cluster.unset_event("EtChangeQueryPriorityDebugInfo")


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_QMR_GUCS)
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestBurstQueryPriority(BurstTest):
    def test_burst_query_priority_succ(self, cluster, db_session):
        """
        This tests that the priority value is propagated to burst successfully.
        """
        with db_session.cursor() as cursor, set_event_priority(cluster):
            cursor.execute("set query_group to burst;")
            cursor.execute("select /**DP-17446-burst**/ count(*) "
                           "from catalog_sales;")
            cursor.execute("select pg_backend_pid();")
            output = cursor.fetchall()
            pid = output[0][0]
        sleep(10)
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select query from stl_query where "
                                     "querytxt ilike '%DP-17446-burst%' and "
                                     "pid = {} order by starttime desc "
                                     "limit 1".format(pid))
        query = int(rows[0][0])
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select query_priority from stl_wlm_query"
                                     " where query = {}".format(query))
        # Verify that the priority obtained in the main side is "high"
        assert rows[0][0].find('high') != 1

        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select concurrency_scaling_query from "
                                     "stl_concurrency_scaling_query_mapping "
                                     "where primary_query={}".format(query))
        brust_qid = rows[0][0]
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select trim(message) as message from "
                                     "stl_event_trace where event_name = "
                                     "'EtChangeQueryPriorityDebugInfo' and "
                                     "message ilike '%Burst query:{}%' order "
                                     "by eventtime limit 1;".format(brust_qid))
        # The bursted query should have the priority 2, i.e., 'High'.
        assert rows[0][0].find('priority:2') != -1


    def test_burst_query_priority_fail(self, cluster, db_session):
        """
        This is a negative test.
        """
        with db_session.cursor() as cursor, set_event_priority(cluster):
            cursor.execute("set query_group to noburst;")
            cursor.execute("select /**DP-17446-noburst*/ count(*) "
                           "from catalog_sales;")
        sleep(10)
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select query from stl_query where "
                                     "querytxt like '%DP-17446-noburst%' "
                                     "order by starttime desc limit 1")
        query = int(rows[0][0])
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select trim(message) as message from "
                                     "stl_event_trace where event_name = "
                                     "'EtChangeQueryPriorityDebugInfo' and "
                                     "message like '%Burst query:{}%' order "
                                     "by eventtime limit 1;".format(query))
        # This is a negative test where query should not be bursted, so there
        # should have no record here.
        assert len(rows) == 0


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_MANUAL_GUCS)
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestBurstQueryPriorityManualWLM(BurstTest):
    def test_burst_query_priority_manual_wlm(self, cluster, db_session):
        """
        This tests that the priority value is converted to the default value
        in burst cluster when the priority obtained from main is invalid.
        """
        with db_session.cursor() as cursor, set_event_priority(cluster):
            cursor.execute("set query_group to burst;")
            cursor.execute("select /**DP-18803**/ count(*) "
                           "from catalog_sales;")
            cursor.execute("select pg_backend_pid();")
            output = cursor.fetchall()
            pid = output[0][0]
        sleep(10)
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select query from stl_query where "
                                     "querytxt ilike '%DP-18803%' and pid = {}"
                                     "order by starttime desc "
                                     "limit 1".format(pid))
        query = int(rows[0][0])
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select query_priority from stl_wlm_query"
                                     " where query = {}".format(query))
        # Verify that the priority obtained in the main side is "n/a".
        assert rows[0][0].find('n/a') != -1

        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select concurrency_scaling_query from "
                                     "stl_concurrency_scaling_query_mapping "
                                     "where primary_query={}".format(query))
        brust_qid = rows[0][0]
        rows = run_priviledged_query(cluster, self.db.cursor(),
                                     "select trim(message) as message from "
                                     "stl_event_trace where event_name = "
                                     "'EtChangeQueryPriorityDebugInfo' and "
                                     "message ilike '%Burst query:{}%' order "
                                     "by eventtime limit 1;".format(brust_qid))
        # The bursted query should have the priority 3, i.e., 'Normal', even
        # the priority obtained from the main size is -1 (n/a).
        assert rows[0][0].find('priority:3') != -1
