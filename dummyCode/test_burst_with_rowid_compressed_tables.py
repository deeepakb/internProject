# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_write import BurstWriteTest, burst_write_mv_gucs
from raff.common.db.session import DbSession
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = "INSERT INTO {sch}.{tbl} SELECT * FROM {sch}.{tbl}"
CREATE_STMT = "CREATE TABLE {}.{} (c0 int, c1 int)"
INSERT_CMD = "INSERT INTO {}.{} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
DROP_CMD = "DROP TABLE IF EXISTS {}.{}"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_mv_gucs.items()))
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_rowid_compression_with_az64': 'true',
        'calc_vacuum_frag_from_block_len': 'true'
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.session_ctx(user_type='bootstrap')
class TestBurstWriteCompressedROWIDTables(BurstWriteTest):

    def _setup_tables(self, db_session, schema, base_tbl_name):
        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(schema, base_tbl_name))
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            for i in range(10):
                cursor.execute(
                    INSERT_SELECT_CMD.format(sch=schema, tbl=base_tbl_name))

    def _verify_table_content_rowid_num_values(self,
                                               cursor,
                                               table,
                                               num_tail_blocks=6):
        cursor.execute("set query_group to metrics;")
        rowid_bl_v_validation_query_compressed = (
            "select case when count(*) < {} then 'correct' else 'wrong' end"
            " as rowid_validation from (select num_values from "
            "stv_blocklist where tbl=(select id from stv_tbl_perm where "
            "name=\'{}\' limit 1) and col=3 and num_values<{});")
        # the number of values that a one compressed rowid block can hold
        num_values = 95500
        cursor.execute(
            rowid_bl_v_validation_query_compressed.format(
                num_tail_blocks + 1, table, num_values))
        assert cursor.fetchall() == [('correct', )]

    def test_burst_write_create_compressed_rowid_table_burst(self, cluster):
        """
        Test: We would test out that compressed rowid tables created on burst
        cluster is propagated back to main cluster and there aren't assumptions
        that ROWID is not encoded..
        1. create table on burst.
        2. bursted write;
        3. check table on main has correct rowid encoding
        """
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema

        base_tbl_name = 'dp37746_t1'
        try:
            with db_session_master.cursor() as cursor:
                cursor.execute("set query_group to burst;")
                self._setup_tables(db_session_master, schema, base_tbl_name)
                self._start_and_wait_for_refresh(cluster)

                for i in range(10):
                    log.info("Burst insert iteration {} {}".format(
                        base_tbl_name, i))
                    cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
                    self._check_last_query_bursted(cluster, cursor)
                    cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
                    self._check_last_query_bursted(cluster, cursor)

                cursor.execute("set query_group to metrics;")
                cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
                self._check_last_query_didnt_burst(cluster, cursor)
                tbl_name = "{}.{}".format(schema, base_tbl_name)

                # verify that table has the correct rowid encoding
                self._verify_table_content_rowid_num_values(
                    self.db.cursor(), tbl_name)
        finally:
            with self.db.cursor() as cursor:
                cursor.execute(DROP_CMD.format(schema, base_tbl_name))
