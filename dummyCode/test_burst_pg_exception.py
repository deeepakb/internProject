# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

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

CLEANUP_CHECK = """select * from stv_burst_manager_cluster_info where
num_sessions > 0;
"""

LOGGING_CHECK = """select * from stl_burst_query_execution where
action ilike '%ERROR%' and error ilike '%Prematurely failed with PG exception%';
"""


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstPGException(BurstTest):
    @contextmanager
    def event_context(self, cluster, event_name):
        """
        Set event and unset on exit.
        """
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

    def test_burst_pg_exception(self, cluster, db_session):
        """
        This test forcefully introduces a PG exception and checks if we
        correctly cleaned up the burst session.
        """
        with self.event_context(cluster, "EtBurstThrowPGException"):
            # Run burst query with PG exception. It should fail because of
            # the elog error.
            with self.burst_db_session(db_session) as cursor:
                with pytest.raises(Exception):
                    cursor.execute(QUERY)

        # Now check we correctly cleaned up and logged the exception.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(CLEANUP_CHECK)
            assert bootstrap_cursor.fetchall() == []
            bootstrap_cursor.execute(LOGGING_CHECK)
            bootstrap_cursor.fetchall()
            assert bootstrap_cursor.rowcount > 0, "PG exception not logged"
