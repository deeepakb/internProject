import pytest
import uuid

from contextlib import contextmanager
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "verify_query_bursted"]

TEST_OID_NULLABILITY_SETUP = """
 create table date_ssb1_burst(
   d_datekey           integer       not null DistKey SortKey,
   d_date              character(18) not null,
   d_dayofweek         character(9)  not null,
   d_month             character(9)  not null,
   d_year              integer       not null,
   d_yearmonthnum      integer       not null,
   d_yearmonth         character(7)  not null,
   d_daynuminweek      integer       not null,
   d_daynuminmonth     integer       not null,
   d_daynuminyear      integer       not null,
   d_monthnuminyear    integer       not null,
   d_weeknuminyear     integer       not null,
   d_sellingseason     character(12) not null,
   d_lastdayinweekfl   boolean       not null,
   d_lastdayinmonthfl  boolean       not null,
   d_holidayfl         boolean       not null,
   d_weekdayfl         boolean       not null
 );

 insert into date_ssb1_burst values
 (1,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true),
 (2,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true),
 (3,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true),
 (4,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true),
 (5,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true),
 (6,'2018-06','Sunday','June',2018,2018,
  '2018',1,1,1,1,1,'Spring',true,true,true,true);

 analyze date_ssb1_burst;
"""

TEST_OID_NULLABILITY_CLEANUP = """
 drop table date_ssb1_burst;
"""


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstNullability(BurstTest):
    """
    Test for nullability check on Burst.
    """
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def run_ported_test(self, db_session, cluster, setup_queries,
                        cleanup_queries, testfile):
        """
        Run ported tests which create testing tables.
        """
        # Set up testing tables and backup.
        with db_session.cursor() as cursor, self.auto_release_local_burst(
                cluster):
            cursor.execute(setup_queries)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Run test workload.
        try:
            with self.burst_db_session(db_session) as cursor:
                self.execute_test_file(testfile,
                                       session=db_session)
        finally:
            with db_session.cursor() as cursor, self.auto_release_local_burst(
                    cluster):
                cursor.execute(cleanup_queries)

    def test_burst_oid_nullability(self, db_session, cluster,
                                   verify_query_bursted):
        """
        Test to verify we correctly set the system columns' nullability, such
        as oid, insertxid and deletexid.
        """
        self.run_ported_test(db_session, cluster, TEST_OID_NULLABILITY_SETUP,
                             TEST_OID_NULLABILITY_CLEANUP,
                             'burst_oid_nullability')
