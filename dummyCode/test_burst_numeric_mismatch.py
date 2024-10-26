# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import uuid

from contextlib import contextmanager
from decimal import Decimal

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.host_type import HostType
from raff.util.utils import run_bootstrap_sql
from raff.common.db.session_context import SessionContext
from raff.common.cluster.cluster_session import ClusterSession

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

QUERY = """select /* TPC-DS query96.tpl 0.1 */ top 100 count(*)
from store_sales
    ,household_demographics
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by count(*)
;
"""

LOGGING_CHECK = """select * from stl_burst_query_execution where
action ilike '%ERROR%' and error ilike '%Numeric format mismatch detected%';
"""

@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstNumericFormatMismatch(BurstTest):
    @contextmanager
    def event_context(self, cluster, event_name):
        try:
            cluster.set_event(event_name)
            yield
        finally:
            cluster.unset_event(event_name)

    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def test_numeric_format_mismatch(self, cluster, db_session):
        """
        Verify that query cannot be bursted if there is a numeric
        format mismatch between main and burst cluster.
        """
        # Check that exception is thrown if numeric format mismatch detected.
        with self.event_context(cluster, "EtChangeNumericFormat"):
            with self.burst_db_session(db_session) as cursor:
                with pytest.raises(Exception):
                    cursor.execute(QUERY)

        # Check that expected exception was thrown.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(LOGGING_CHECK)
            bootstrap_cursor.fetchall()
            assert bootstrap_cursor.rowcount > 0, "No exception was thrown"
