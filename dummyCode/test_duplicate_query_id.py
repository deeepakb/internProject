import logging
import pytest
import uuid
from raff.monitoring.monitoring_test import MonitoringTestSuite


log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestDuplicateQueryId(MonitoringTestSuite):
    def run_query(self, cluster_session, db_session, query, return_result=False):
        with cluster_session():
            with db_session.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall() if return_result else []

    def test_duplidate_query_id(self, cluster, cluster_session, db_session):
        """
        This test verifies if there is duplicate query id in stl_query
        """
        test_table = "test_tbl_{}".format(str(uuid.uuid4())[:8])
        query1 = """
            create table {tbl} (a integer);
            insert into {tbl} values (1);
            insert into {tbl} values (2);
            insert into {tbl} values (2);
            select count(*) from {tbl};
            select count(a) from {tbl};
        """.format(tbl=test_table)
        query2 = """
            select a+1, a+2 from {tbl} order by 1,2;
            select a, count(*) from {tbl} group by a order by 1,2;
        """.format(tbl=test_table)
        query3 = """
            select sum(a) from {tbl};
        """.format(tbl=test_table)
        try:
            for query in [query1, query2, query3]:
                self.run_query(cluster_session, db_session, query)
            duplicate_query_ids = self.run_query(
                cluster_session, db_session,
                """
                select query, count(*) from stl_query
                where query >= 2 group by query having count(*) > 1;
                """, return_result=True)
            assert not duplicate_query_ids, \
                "Found duplicate query id, {}".format(duplicate_query_ids)
        finally:
            self.run_query(
                cluster_session, db_session,
                "drop table if exists {}".format(test_table))
