import logging
import pytest
import getpass
import uuid
import json


from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

QUERY_CONCURRENCY = 2
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
WLM_CFG = [{
    "query_group": ["burst"],
    "user_group": ["burst"],
    "concurrency_scaling": "auto",
    "query_concurrency": QUERY_CONCURRENCY
}, {
    "query_group": ["noburst"],
    "user_group": ["noburst"],
    "query_concurrency": 5
}]
GUCS = dict(
    wlm_json_configuration=json.dumps(WLM_CFG),
    try_burst_first='false',
    burst_enable_write_user_ctas='true',
    burst_enable_write_user_temp_ctas='false',
    burst_enforce_user_temp_table_complete_rerep_to_s3='false')
VALIDATION_SQL = """SELECT SUM(col_smallint_raw),
                    SUM(col_smallint_bytedict),
                    SUM(col_smallint_delta) from
                    {}.{};"""


@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
         gucs=dict(list(burst_user_temp_support_gucs.items()) +
                   list(GUCS.items())))
@pytest.mark.custom_local_gucs(
         gucs=dict(list(burst_user_temp_support_gucs.items()) +
                   list(GUCS.items())))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempExecutionCriteria(BurstTempWrite):

    def test_burst_temp_after_main_queue_full(self, cluster):
        """
        1. Load all shaped temp tables
        2. Set event to simulate all queues on main are full,
           where we are forced to burst
        3. Execute READ/WRITE queries on temp tables
        4. Validate query referencing temp tables gets bursted
        5. Validate data blocks referenced by temp table are uploaded to S3
        6. Validate data integrity and table metadata properties
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, cursor,
                                                   max_tables_to_load=1)
            main_result = []
            for temp_table_name, temp_table_schema in temp_table_dict.items():
                cursor.execute(VALIDATION_SQL.format
                               (temp_table_schema, temp_table_name))
                main_result.append(cursor.fetchall())
                self._check_last_query_didnt_burst(cluster, cursor)
            with self._main_queue_full(cluster, 600):
                burst_result = []
                for temp_table_name, temp_table_schema in temp_table_dict.items():
                    cursor.execute(VALIDATION_SQL.format(
                        temp_table_schema, temp_table_name))
                    burst_result.append(cursor.fetchall())
                    qid = self.last_query_id(cursor)
                    self._check_last_query_bursted(cluster, cursor)
                    boot_cursor.execute(
                        "select blocks_not_in_s3 from "
                        "stl_burst_user_temp_table_stats where "
                        "query = {} and query_exec_state ilike '%{}%'"
                        .format(qid, "S3ReplicationComplete")
                    )
                    blocks_not_in_s3 = boot_cursor.fetch_scalar()
                    assert blocks_not_in_s3 == 0
                    self._validate_table(cluster, temp_table_schema,
                                         temp_table_name, "distkey")
                assert main_result == burst_result

    def test_run_temp_on_main_after_main_queue_full(self, cluster):
        """
        1. Load all shaped temp tables
        2. Set event to to defer async rerep requests,
           so that async upload to s3 is not completed
           and query eventually finds a free slot on main
        3. Execute READ/WRITE queries on temp tables
        4. Validate query referencing temp tables runs on main as the data
           block upload to S3 took longer than finding a free slot on main.
        5. Validate data blocks referenced by temp table are not uploaded to S3
        6. Validate data integrity and table metadata properties
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor, \
                cluster.event('EtAsyncRerepDeferIssuingAsyncS3RerepRequests'):
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, cursor,
                                                   max_tables_to_load=1)
            with self._main_queue_full(cluster):
                for temp_table_name, temp_table_schema in temp_table_dict.items():
                    cursor.execute("select count(*) from {}.{}"
                                   .format(temp_table_schema,
                                           temp_table_name))
                    qid = self.last_query_id(cursor)
                    self._check_last_query_didnt_burst(cluster, cursor)
                    boot_cursor.execute(
                            "select max(total_blocks), min(blocks_not_in_s3) from "
                            "stl_burst_user_temp_table_stats where "
                            "query = {} and query_exec_state ilike '%{}%'"
                            .format(qid, "PendingS3Replication")
                        )
                    rows = boot_cursor.fetchall()
                    for row in rows:
                        [total_blocks, blocks_not_in_s3] = row
                        log.info("blocks not in S3: %d", blocks_not_in_s3)
                        assert blocks_not_in_s3 == total_blocks
                    self._validate_table(cluster, temp_table_schema,
                                         temp_table_name, "distkey")
