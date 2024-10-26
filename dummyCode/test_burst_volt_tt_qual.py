# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from contextlib import contextmanager
import datetime
import pytest
import uuid

from raff.burst.burst_test import (
    BurstTest, setup_teardown_burst, verify_query_didnt_burst,
    verify_all_queries_bursted, get_burst_cluster_arn)
from raff.common.cred_helper import get_role_auth_str
from raff.util.utils import run_bootstrap_sql
from raff.common.profile import AwsAccounts
from raff.burst.burst_write import BurstWriteTest

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [
    "setup_teardown_burst", "verify_query_didnt_burst",
    "verify_all_queries_bursted"
]

CUSTOM_GUCS = {
    'burst_mode': '3',
    'burst_enable_volt_tts': 'true',
    'burst_volt_tts_require_replay': 'false',
    'burst_enable_write_user_ctas': 'false',
    'burst_enable_write_user_temp_ctas': 'false'
}

TPCDS_Q1_SQL = """
      {}
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""
TPCDS_Q1_SQL_DIRTY = """
      {}
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer_copy
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""
TPCDS_Q1_SQL_STL = """
    {}
    WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
        (SELECT sr_customer_sk AS ctr_customer_sk,
                sr_store_sk AS ctr_store_sk,
                sum(SR_STORE_CREDIT) AS ctr_total_return
         FROM store_returns,
              date_dim
         WHERE sr_returned_date_sk = d_date_sk
           AND d_year =2000
         GROUP BY sr_customer_sk,
                  sr_store_sk)
      SELECT /* TPC-DS query1.tpl 0.12 */ top 100 label
      FROM customer_total_return ctr1,
           store,
           stl_query
      WHERE ctr1.ctr_total_return >
          (SELECT avg(ctr_total_return)*1.2
           FROM customer_total_return ctr2
           WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
        AND s_store_sk = ctr1.ctr_store_sk
        AND s_state = 'MI'
        AND ctr1.ctr_customer_sk = query
        AND label = 'FABIAN'
      ORDER BY label
"""
TPCDS_Q1_STORED_PROC1 = """
    CREATE OR REPLACE PROCEDURE proc1()
    LANGUAGE plpgsql
    AS $$
    BEGIN
      EXECUTE '{}';
    END;
    $$;
"""
TPCDS_Q1_STORED_PROC2 = """
    CREATE OR REPLACE PROCEDURE proc2()
    LANGUAGE plpgsql
    AS $$
    BEGIN
      EXECUTE '{}';
      EXECUTE '{}';
    END;
    $$;
"""
TPCDS_Q1_STORED_PROC3 = """
    CREATE OR REPLACE PROCEDURE proc3()
    LANGUAGE plpgsql
    AS $$
    DECLARE
      result BIGINT;
    BEGIN
      {}
      RAISE INFO 'Result: %', result;
    END;
    $$;
"""
TPCDS_Q1_STORED_PROC4 = """
    CREATE OR REPLACE PROCEDURE proc4()
    LANGUAGE plpgsql
    AS $$
    DECLARE
      result RECORD;
    BEGIN
      FOR result IN {} LOOP
        RAISE INFO 'Result: %', result.c_customer_id;
      END LOOP;
    END;
    $$;
"""
TPCDS_Q1_STORED_PROC5 = """
    CREATE OR REPLACE PROCEDURE proc5()
    LANGUAGE plpgsql
    AS $$
    DECLARE
      c1 REFCURSOR;
      result BIGINT;
    BEGIN
      OPEN c1 FOR {}
      FETCH c1 into result;
      RAISE INFO 'Result: %', result;
      CLOSE c1;
    END;
    $$;
"""


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestVoltTTBurstQualification(BurstTest):
    """
    Tests various operations that are not supported when trying to
    burst queries that generate Volt temporary tables.
    """

    @contextmanager
    def auto_disable_refresh(self, cluster):
        cluster.run_xpx("burst_disable_refresh")
        yield
        cluster.run_xpx("burst_enable_refresh")

    def test_burst_volt_tt_qual_explain(self, db_session,
                                        verify_query_didnt_burst):
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL.format("EXPLAIN "))
            cursor.fetchall()

    def test_burst_volt_tt_qual_CTAS(self, cluster, db_session):
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(
                TPCDS_Q1_SQL.format("CREATE TEMPORARY TABLE t1 AS "))
            burst_write_test = BurstWriteTest()
            burst_write_test._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("SELECT COUNT(*) FROM t1;")
            assert cursor.fetchall() == [(0, )]
            cursor.execute("DROP TABLE t1;")

    def test_burst_volt_tt_qual_cursor(self, db_session,
                                       verify_query_didnt_burst):
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute("BEGIN;")
            cursor.execute(TPCDS_Q1_SQL.format("DECLARE c1 CURSOR FOR "))
            cursor.execute("FETCH ALL FROM c1;")
            assert cursor.fetchall() == []
            cursor.execute("COMMIT;")

    def test_no_aqmv_if_burst_sticky_session(self, cluster, db_session):
        """
        Tests that AQMV will not rewrite a volt decorrelated sub-query to use
        an MV if first volt query creates a sticky session.
        """
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute('CREATE TABLE base1 (a int);')
            cursor.execute('CREATE TABLE base2 (a int);')
            cursor.execute('INSERT INTO base1 VALUES (1);')
            cursor.execute('INSERT INTO base2 VALUES (1);')

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        snapshot_identifier = ("{}-{}".format(cluster.cluster_identifier,
                                              str(uuid.uuid4().hex)))
        cluster.backup_cluster(snapshot_identifier)
        self.wait_for_refresh_to_start(cluster, start_str,
                                       get_burst_cluster_arn(cluster))

        # Make sure burst cluster has an old back up by running a query on an
        # old table.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute('set enable_result_cache_for_session to false')
            cursor.execute('SELECT DISTINCT a FROM base1')
            self.check_last_query_bursted(cluster, cursor)

        # Disable refresh so that after MV is created, burst cluster is not
        # refreshed with a backup which contains MV.
        with self.auto_disable_refresh(cluster):
            # Create a new MV, so that it can be used for rewriting.
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute('CREATE MATERIALIZED VIEW mv2 AS '
                               'SELECT a FROM base2;')

            # Make sure there was no rewriting and that query bursted successfully.
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute('SET mv_enable_aqmv_costing to false')
                cursor.execute('set enable_result_cache_for_session to false')
                cursor.execute('SELECT DISTINCT a FROM base1 '
                               'UNION ALL '
                               'SELECT DISTINCT a FROM base1 '
                               'UNION ALL '
                               'SELECT a FROM base2; ')
                cursor.execute('SELECT pg_last_query_id()')
                qid = cursor.fetch_scalar()
                assert qid > -1, ("Query Id: {}").format(qid)
                self.check_last_query_bursted(cluster, cursor)

                # Query was not rewritten to use MV.
                cnt = run_bootstrap_sql(
                    cluster, ("SELECT count(*) "
                              "FROM stl_mv_aqmv a "
                              ", stl_plan_qid_map m "
                              ", stl_wlm_query q "
                              "WHERE a.query = m.plan_qid "
                              "AND m.query = q.query "
                              "AND a.aqmv_status = 1 "
                              "AND q.query = {} ").format(qid))[0][0]
                assert int(cnt) == 0, ("Was AQMV successful {}").format(cnt)

                # Query did scan a volt tt.
                res = run_bootstrap_sql(cluster,
                                        ("SELECT volt_temp_tables_accessed "
                                         "FROM stl_internal_query_details "
                                         "WHERE query = {} ").format(qid))
                assert len(res) > 0, ("Length of result: {}").format(len(res))
                assert int(res[0][0]) == 2, ("Number of volt tt "
                                             "used in scan: {}").format(
                                                 res[0][0])

    def test_burst_volt_tt_qual_prepare(self, db_session,
                                        verify_all_queries_bursted):
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL.format("PREPARE p1 AS "))
            cursor.execute(TPCDS_Q1_SQL.format("PREPARE p2 AS "))
            cursor.execute("EXECUTE p2;")
            assert cursor.fetchall() == []
            cursor.execute(TPCDS_Q1_SQL.format("PREPARE p3 AS "))
            cursor.execute("EXECUTE p1;")
            assert cursor.fetchall() == []
            cursor.execute("DEALLOCATE p2;")
            cursor.execute("EXECUTE p3;")
            cursor.execute("EXECUTE p3;")
            cursor.execute("EXECUTE p3;")
            assert cursor.fetchall() == []
            cursor.execute("DEALLOCATE p3;")
            cursor.execute("DEALLOCATE p1;")

    def test_burst_volt_tt_qual_unload(self, db_session, s3_client,
                                       verify_all_queries_bursted):
        IAM_CREDENTIAL = get_role_auth_str(
            AwsAccounts.DP.iam_roles.Redshift_S3_Write)
        TEST_S3_PATH = ("s3://cookie-monster-s3-ingestion/"
                        "raff_test_burst_unload/{}/{}/")
        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format('volt_tt_quals', rand_str)
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                # Note: UNLOAD does not support LIMIT.
                cursor.run_unload(
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'").replace(
                        " top 100", ""), unload_path, IAM_CREDENTIAL)
                assert cursor.last_unload_row_count() == 0

    def test_burst_volt_tt_qual_dirty(self, db_session,
                                      verify_query_didnt_burst):
        # Create not backed-up dataset and verify that the query against dirty
        # data was not bursted. The query is written so that the dirty table is
        # only referenced by the rewritten query (not by the Volt-generated
        # CTAS).
        CREATE_TABLE_SQL = """CREATE TABLE customer_copy AS
                              SELECT * FROM customer;"""
        DROP_TABLE_SQL = 'DROP TABLE customer_copy;'
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(TPCDS_Q1_SQL_DIRTY.format(""))
            assert cursor.fetchall() == []
            cursor.execute(DROP_TABLE_SQL)

    def test_burst_volt_tt_qual_stl(self, db_session,
                                    verify_query_didnt_burst):
        # Verify that the query against a system table is not bursted. The
        # query is written so that the system table is only referenced by
        # the rewritten query (not by the Volt-generated CTAS).
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL_STL.format(""))
            assert cursor.fetchall() == []

    def test_burst_volt_tt_qual_view_dirty(self, db_session,
                                           verify_query_didnt_burst):
        # Create not backed-up dataset and verify that the query against dirty
        # data was not bursted. The query is written so that the dirty table is
        # only referenced by the rewritten query (not by the Volt-generated
        # CTAS).
        CREATE_TABLE_SQL = """CREATE TABLE customer_copy AS
                              SELECT * FROM customer;"""
        DROP_TABLE_SQL = 'DROP TABLE customer_copy;'
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(TPCDS_Q1_SQL_DIRTY.format("CREATE VIEW v1 AS "))
            cursor.execute("SELECT * FROM v1;")
            assert cursor.fetchall() == []
            cursor.execute("DROP VIEW v1;")
            cursor.execute(DROP_TABLE_SQL)

    def test_burst_volt_tt_qual_view_stl(self, db_session,
                                         verify_query_didnt_burst):
        # Verify that the query against a system table is not bursted. The
        # query is written so that the system table is only referenced by
        # the rewritten query (not by the Volt-generated CTAS).
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL_STL.format("CREATE VIEW v2 AS "))
            cursor.execute("SELECT * FROM v2;")
            assert cursor.fetchall() == []
            cursor.execute("DROP VIEW v2;")

    def test_burst_volt_tt_qual_disabled_funcs(self, db_session,
                                               verify_query_didnt_burst):
        # Verify that a query using disabled functions is not bursted.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(
                TPCDS_Q1_SQL.format("").replace(
                    "'MI'", "pg_last_query_id()::varchar(30)"))
            assert cursor.fetchall() == []

    def test_burst_volt_tt_qual_udf(self, db_session,
                                    verify_query_didnt_burst):
        # Verify that a query using python UDF functions is not bursted.
        UDF_SQL = """CREATE OR REPLACE FUNCTION f_json_ok(js varchar(65535))
                     RETURNS varchar IMMUTABLE AS $$
                       return (js + 'Test')[:4]
                     $$ LANGUAGE plpythonu;"""
        with self.db.cursor() as cursor:
            cursor.execute("grant usage on language plpythonu to PUBLIC;")
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(UDF_SQL)
            cursor.execute(
                TPCDS_Q1_SQL.format("").replace(
                    "s_state", "f_json_ok(s_state)::varchar(30)"))
            assert cursor.fetchall() == []
            cursor.execute("DROP FUNCTION f_json_ok(js varchar(65535));")

    def test_burst_volt_tt_qual_stored_proc_pos(self, db_session,
                                                verify_all_queries_bursted):
        with self.burst_db_cursor(db_session) as cursor:
            # Run first stored procedure.
            cursor.execute(
                TPCDS_Q1_STORED_PROC1.format(
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'")))
            cursor.execute("CALL proc1();")
            cursor.execute("DROP PROCEDURE proc1();")
            # Run second stored procedure.
            cursor.execute(
                TPCDS_Q1_STORED_PROC2.format(
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'"),
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'")))
            cursor.execute("CALL proc2();")
            cursor.execute("DROP PROCEDURE proc2();")
            # Run third stored procedure.
            cursor.execute(
                TPCDS_Q1_STORED_PROC3.format(
                    TPCDS_Q1_SQL.format("")
                    .replace("top 100 c_customer_id",
                             "top 100 c_customer_id INTO result")))
            cursor.execute("CALL proc3();")
            cursor.execute("DROP PROCEDURE proc3();")

    def test_burst_volt_tt_qual_stored_proc_neg(self, db_session,
                                                verify_query_didnt_burst):
        with self.burst_db_cursor(db_session) as cursor:
            # Run stored procedure using an implicit cursor should not burst.
            cursor.execute(
                TPCDS_Q1_STORED_PROC4.format(
                    TPCDS_Q1_SQL.format("").replace(";", "")))
            cursor.execute("CALL proc4();")
            cursor.execute("DROP PROCEDURE proc4();")
            # A stored procedure using a cursor should not burst.
            cursor.execute(
                TPCDS_Q1_STORED_PROC5.format(TPCDS_Q1_SQL.format("")))
            cursor.execute("CALL proc5();")
            cursor.execute("DROP PROCEDURE proc5();")
