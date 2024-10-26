# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.burst.burst_status import BurstStatus

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CTAS_STMT = ("CREATE TABLE ctas1 AS" " (select i from ctas_test_table)")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true'
})
@pytest.mark.custom_local_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true',
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCTASAlterEncode(BurstWriteTest):
    def _setup_tables(self, cursor):
        tbl_def = ("CREATE TABLE ctas_test_table (i varchar(2000) ENCODE LZO) "
                   "diststyle key distkey(i)")
        cursor.execute(tbl_def)
        cursor.execute(
            "INSERT INTO ctas_test_table values ('1'), ('7'), ('10');")

    def _verify_ctas_table(self, cursor, res):
        cursor.execute("set query_group to noburst")
        cursor.execute("SELECT * FROM ctas1")
        expected_res = cursor.fetchall()
        assert expected_res == res

    def _get_ctas_qid(self, cursor):
        query = ("select query from stl_query where querytxt like '%{}%' "
                 "order by query desc limit 1;")
        cursor.execute(query.format(CTAS_STMT))
        qid = cursor.fetch_scalar()
        return qid

    def _run_and_return_ctas_result(self, cursor, cluster):
        cursor.execute("set query_group to burst")
        log.info("Running CTAS")
        cursor.execute(CTAS_STMT)
        # Verify auto analyze was running on main
        self._check_last_query_didnt_burst_with_code(
            cluster, cursor, BurstStatus.in_eligible_query_type)
        # Verify CTAS query was running on burst
        qid = self._get_ctas_qid(cursor)
        log.info("Done CTAS qid: {}".format(qid))
        self.verify_query_bursted(cluster, qid)
        return self._get_table_contents(cursor, cluster, "ctas1", False)

    def _get_table_contents(self, cursor, cluster, table, is_burst):
        if is_burst:
            cursor.execute("set query_group to burst")
        else:
            cursor.execute("set query_group to noburst")
        cursor.execute("SELECT * from {} order by 1".format(table))
        res = cursor.fetchall()
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        return res

    def _perform_dmls_on_burst(self, cursor, cluster):
        cursor.execute("set query_group to burst")
        cursor.execute("INSERT INTO ctas_test_table VALUES (1),(10);")
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("INSERT INTO ctas1 VALUES (1),(10);")
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("DELETE FROM ctas1 WHERE i > 7;")
        self._check_last_query_bursted(cluster, cursor)

    def _perform_dmls_on_main(self, cursor):
        cursor.execute("set query_group to noburst")
        cursor.execute("INSERT INTO ctas_test_table VALUES (1),(10);")
        cursor.execute("INSERT INTO ctas1 VALUES (1),(10);")
        cursor.execute("DELETE FROM ctas1 WHERE i > 7;")

    def _drop_ctas_table(self, cursor):
        cursor.execute("DROP TABLE ctas1;")

    def test_burst_ctas_alter_encode(self, cluster, db_session):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            cursor.execute("SET query_group TO BURST;")
            self._setup_tables(cursor)
            self._start_and_wait_for_refresh(cluster)
            res = self._run_and_return_ctas_result(cursor, cluster)
            assert res == self._get_table_contents(cursor, cluster, "ctas1",
                                                   True)
            encode_check = ("""
                            SELECT encoding
                            FROM pg_table_def
                            WHERE tablename = 'ctas1'
                            AND "column" = 'i';
                            """)
            cursor.execute(encode_check)
            encode_before_alter = cursor.fetchall()
            # Trigger alter encode on main
            cursor.execute("SET query_group TO NOBURST;")
            cursor.execute("ALTER TABLE ctas1 ALTER COLUMN i encode zstd;")
            cursor.execute(encode_check)
            encode_after_alter = cursor.fetchall()
            log.info("Encode type before: {}, after: {}".format(
                encode_before_alter, encode_after_alter))
            # Insert data based on new encode type on main
            self._perform_dmls_on_main(cursor)
            content_after_alter = self._get_table_contents(
                cursor, cluster, "ctas1", False)
            log.info("ctas table content after alter: {}".format(res))
            # After refresh, check burst read/write on the table
            self._start_and_wait_for_refresh(cluster)
            assert content_after_alter == self._get_table_contents(
                cursor, cluster, "ctas1", True)
            self._perform_dmls_on_burst(cursor, cluster)
            # Verify after alter encode, burst write, content can be read from
            # main and burst clusters.
            content_after_alter = self._get_table_contents(
                cursor, cluster, "ctas1", True)
            assert content_after_alter == self._get_table_contents(
                cursor, cluster, "ctas1", False)
            self._drop_ctas_table(cursor)
