# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from test_burst_rtf_local_scan import (SQL_DDL1, SQL_Q1, RES_Q1, SQL_Q2,
                                       RES_Q2, ERROR_PLAN, RES_Q1_PLAN,
                                       RES_Q2_PLAN, SQL_VALIDATE_PLAN)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

CUSTOM_GUCS = {
    'enable_runtime_filtering': 'true',
    'runtime_filtering_explain': 'true',
    'enable_runtime_filtering_in_burst': 'true',
    'enable_mmf_in_burst': 'true',
    'burst_use_local_scans': 'true'
}

CUSTOM_GUCS_OFF = CUSTOM_GUCS.copy()
CUSTOM_GUCS_OFF.update({'enable_mmf_in_burst': 'false'})

SQL_VALIDATE_MMF = """
SELECT count(*)
FROM stl_mmf_details s
WHERE s.query in (
  select concurrency_scaling_query
  from stl_concurrency_scaling_query_mapping
  where primary_query = {})
"""

ERROR_MMF = "unexpected min/max filter stats"
SQL_DROP = """
             DROP TABLE IF EXISTS dist_rtf_t1;
             DROP TABLE IF EXISTS dist_rtf_t2;
             DROP TABLE IF EXISTS dist_rtf_t3;
            """
TPCH8 = """
        SELECT o_year,
            sum(CASE
                    WHEN nation = 'BRAZIL' THEN volume
                    ELSE 0
                END) / sum(volume) AS mkt_share
        FROM
        (SELECT --date_part('year',o_orderdate) as o_year,
        date_part_year(o_orderdate::date) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
        FROM tpch1_part_redshift,
                tpch1_supplier_redshift,
                tpch1_lineitem_redshift, tpch1_orders_redshift,
                                    tpch1_customer_redshift,
                                    tpch_nation_redshift n1,
                                    tpch_nation_redshift n2,
                                    tpch_region_redshift
        WHERE p_partkey = l_partkey
            AND s_suppkey = l_suppkey
            AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey
            AND r_name = 'AMERICA'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
            AND p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
        GROUP BY o_year
        ORDER BY o_year;
        """


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstMMFLocalScan(BurstTest):
    # Test MMF in burst.
    def test_burst_mmf_local_scan(self, cursor, db_session, cluster):
        # Prepare data.
        with db_session.cursor() as cursor, \
             self.auto_release_local_burst(cluster):
            cursor.execute(SQL_DROP)
            cursor.execute(SQL_DDL1)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Execute Q1 and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(SQL_Q1)
            assert cursor.fetchall() == RES_Q1
            qid = cursor.last_query_id()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_PLAN.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q1_PLAN, \
                ERROR_PLAN
            bootstrap_cursor.execute(SQL_VALIDATE_MMF.format(qid))
            assert bootstrap_cursor.fetch_scalar() != 0, \
                ERROR_MMF

        # Execute Q2 and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(SQL_Q2)
            assert cursor.fetchall() == RES_Q2
            qid = cursor.last_query_id()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_PLAN.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q2_PLAN, \
                ERROR_PLAN
            bootstrap_cursor.execute(SQL_VALIDATE_MMF.format(qid))
            assert bootstrap_cursor.fetch_scalar() != 0, \
                ERROR_MMF

        # Clean up
        with db_session.cursor() as cursor:
            cursor.execute(SQL_DROP)


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstMMFNoPred(BurstTest):
    # Test MMF in Burst when query has no predicate.
    SQL_DROP_NO_PRED = """
        DROP TABLE IF EXISTS dp37771_users;
        DROP TABLE IF EXISTS dp37771_deps;
    """
    SQL_DDL_NO_PRED = """
        DROP TABLE IF EXISTS dp37771_users;
        DROP TABLE IF EXISTS dp37771_deps;

        CREATE TABLE IF NOT EXISTS dp37771_users (id int , depID int)
         DISTSTYLE even SORTKEY (id, depID);

        INSERT INTO dp37771_users (id, depID) VALUES (0, 1),(1, 1),(2, 0),
        (3, 1),(4, 2),(5, 1),(6, 3),(7, 1);

        CREATE TABLE IF NOT EXISTS
        dp37771_deps (depID int SORTKEY, depName VARCHAR)
        DISTSTYLE even;

        INSERT INTO dp37771_deps (depID, depName) VALUES
        (0, 'Test0'),(1, 'Test1'),(2, 'Test1'),(3, 'Test1');
        """

    NO_PRED_Q = """
    SELECT * FROM dp37771_users, dp37771_deps
    WHERE dp37771_users.depID = dp37771_deps.depID
    ORDER BY 1;
    """

    RES_NO_PRED_Q = [(0, 1, 1, "Test1"), (1, 1, 1, "Test1"),
                     (2, 0, 0, "Test0"), (3, 1, 1, "Test1"), (4, 2, 2,
                                                              "Test1"),
                     (5, 1, 1, "Test1"), (6, 3, 3, "Test1"), (7, 1, 1,
                                                              "Test1")]

    RES_No_PRED_Q_PLAN = [('scan   ', ), ('project', ), ('bcast  ', ),
                          ('scan   ', ), ('project', ), ('hash   ', ),
                          ('scan   ', ), ('project', ), ('project', ),
                          ('hjoin  ', ), ('project', ), ('sort   ', ),
                          ('scan   ', ), ('return ', ), ('merge  ', ),
                          ('project', ), ('return ', )]

    def test_burst_mmf_no_pred(self, cursor, db_session, cluster):
        with db_session.cursor() as cursor, \
             self.auto_release_local_burst(cluster):
            cursor.execute(self.SQL_DROP_NO_PRED)
            cursor.execute(self.SQL_DDL_NO_PRED)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Execute query and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(self.NO_PRED_Q)
            assert cursor.fetchall() == self.RES_NO_PRED_Q
            qid = cursor.last_query_id()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_PLAN.format(qid))
            assert bootstrap_cursor.fetchall() == self.RES_No_PRED_Q_PLAN, \
                ERROR_PLAN
            bootstrap_cursor.execute(SQL_VALIDATE_MMF.format(qid))
            assert bootstrap_cursor.fetch_scalar() != 0, \
                ERROR_MMF

        with db_session.cursor() as cursor:
            # Clean up.
            cursor.execute(self.SQL_DROP_NO_PRED)


@pytest.mark.localhost_only
@pytest.mark.load_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstMMFTPCH(BurstTest):
    def test_mmf_tpch(self, cursor, db_session, cluster):
        # Here we test MMF in burst are being used with a larger more
        # complicated query.
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Execute query and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCH8)
            cursor.fetchall()
            qid = cursor.last_query_id()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_MMF.format(qid))
            assert bootstrap_cursor.fetch_scalar() != 0, \
                ERROR_MMF


@pytest.mark.localhost_only
@pytest.mark.load_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS_OFF)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstMMFTPCHOff(BurstTest):
    def test_mmf_tpch_off(self, cursor, db_session, cluster):
        # Here we test that there is no MMF in Burst if the feature is off.
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Execute query and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCH8)
            cursor.fetchall()
            qid = cursor.last_query_id()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_MMF.format(qid))
            assert bootstrap_cursor.fetch_scalar() == 0, \
                ERROR_MMF
