# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
BURST_MODE = """
set query_group to burst;
set session_authorization to master;
"""
MSG1 = "There should only be 1 query bursted and collected."
MSG2 = "Query is not collected."
MSG3 = "Query didn't go through cluster size change path."


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.load_tpcds('call_center')
class TestBurstQueryCollection(BurstWriteTest):
    def test_burst_query_predictor_collection(self, cluster):
        """
        1/ Test if burst queries are correctly collected by predictors.
        2/ Test if before burst qureies collection, ClusterSize feature
        has been replace with cluster size the query actually ran.
        """
        self._start_and_wait_for_refresh(cluster)
        with self.db.cursor() as cursor,\
             cluster.event("EtBurstQueryCollection", "level=ElDebug5"):
            cursor.execute(BURST_MODE)
            cursor.execute("""
                SELECT * FROM call_center A, call_center B
                    ORDER BY 1 LIMIT 1;
                """)
            cursor.execute('select pg_last_query_id()')
            qid = cursor.fetch_scalar()
            self.verify_query_bursted(cluster, qid)
            cursor.execute("reset session_authorization;")
            cursor.execute("""
                select message from stl_event_trace where event_name ilike
                    '%EtBurstQueryCollection%' and message ilike '%{}%'
                    order by eventtime desc;
                """.format(qid))
            res = cursor.fetchall()
            # One query bursted and collected by predictor.
            assert len(res) == 2, MSG1
            assert "collected successfully by predictor" in res[0][0], MSG2
            assert "changed feature(ClusterSize)" in res[1][0], MSG3
