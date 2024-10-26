# Copyright 2018 Amazon.com, Inc. or its affiliates
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


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstSerDe(BurstTest):
    @contextmanager
    def auto_reenable(self, cluster):
        """
        Reset burst disabling due to previous error. Also, reset the
        disablement at the end of the test.
        """
        cluster.run_xpx('burst_reenable')
        yield
        cluster.run_xpx('burst_reenable')

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

    def test_version_mismatch(self, cluster, db_session):
        """
        This test forcefully introduces a version mismatch by setting
        EtBurstFailSerDeVersion. It then checks for error.
        """
        # Ensure that test errors don't block future bursting.
        with self.auto_reenable(cluster):
            with self.event_context(cluster, "EtBurstFailSerDeVersion"):
                # Run burst query with version mismatch. Should fail.
                with self.burst_db_session(db_session) as cursor:
                    with pytest.raises(Exception):
                        cursor.execute(QUERY)

            # Run without version mismatch. Should pass.
            with self.burst_db_session(db_session) as cursor:
                cursor.execute(QUERY)

    def test_serializer_cpp_exception(self, cluster, db_session):
        """
        This test forcefully introduces a C++ exception and verifies error.
        """

        msg = "Could not prepare burst query due to serialization error."

        # Ensure that test errors don't block future bursting.
        with self.auto_reenable(cluster):
            with self.event_context(cluster, "EtBurstThrowCppException"):
                # Run burst query that throws C++ exception.
                with self.burst_db_session(db_session) as cursor:
                    try:
                        cursor.execute(QUERY)
                    except Exception as ex:
                        assert msg in str(ex)
