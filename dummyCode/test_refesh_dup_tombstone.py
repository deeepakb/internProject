# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import datetime
import pytest
import uuid
from contextlib import contextmanager

from raff.burst.burst_super_simulated_mode_helper import\
    setup_super_simulate_mode,\
    get_burst_conn_params
from raff.common.dimensions import Dimensions
from raff.common.db.redshift_db import RedshiftDb
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase,\
    DELETE_CMD, COPY_STMT, S3_PATH

log = logging.getLogger(__name__)
__all__ = [setup_super_simulate_mode, disable_all_autoworkers]

CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {};"
UPDATE_CMD = "UPDATE {} set c0 = c0 + 1, c1 = c1 + 1;"

LARGE = "large"
SMALL = "small"

burst_write_no_retry_gucs = {
    'enable_burst_failure_handling': 'false',
    'burst_enable_insert_failure_handling': 'false',
    'burst_enable_delete_failure_handling': 'false',
    'burst_enable_update_failure_handling': 'false',
    'burst_enable_copy_failure_handling': 'false'
}


@pytest.yield_fixture(scope="function")
def super_simulated_mode_method(request, cluster):
    '''
        Setup the super simulated mode for single method. The super-simulated
        cluster will be recycled at the end of the function of this fixture.
    '''
    with setup_super_simulate_mode(request, cluster) as fixture:
        yield fixture


class BaseBurstWriteDupTombstone(TestBurstWriteMixedWorkloadBase):
    def _setup_tables(self, db_session, burst_table, vector):
        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(burst_table, vector.diststyle,
                                   vector.sortkey))
            cursor.execute("begin;")
            cursor.execute(INSERT_CMD.format(burst_table))
            if vector.size == 'large':
                for i in range(10):
                    cursor.execute(
                        INSERT_SELECT_CMD.format(burst_table, burst_table))
            cursor.execute("commit;")

    def _generate_dml_cmd(self, vector, tbl_name):
        if vector.dml == 'insert':
            return INSERT_CMD.format(tbl_name)
        elif vector.dml == 'delete':
            return DELETE_CMD.format(tbl_name)
        elif vector.dml == 'update':
            return UPDATE_CMD.format(tbl_name)
        else:
            return COPY_STMT.format(tbl_name, S3_PATH)

    @contextmanager
    def verify_failed_burst_query_status(self, cluster, dml, status):
        try:
            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            yield
        except Exception as e:
            log.info(e)
        finally:
            query = """
                select query from stl_query
                where userid > 1
                and starttime >= '{}'
                and querytxt ilike '%{}%'
                order by starttime asc
                limit 1
                """.format(start_str, dml)
            with self.db.cursor() as cur:
                cur.execute(query)
                qid = cur.fetch_scalar()
                self.verify_query_status(cluster, qid, status)

    def base_test_burst_write_dup_tombstone(self, cluster, db_session, vector):
        burst_table = "burst_" + str(uuid.uuid4().hex[:8])
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with db_session.cursor() as dml_cursor, \
                burst_session.cursor() as burst_cursor:
            log.info("Setting up tables")
            self._setup_tables(db_session, burst_table, vector)

            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)

            log.info("set event on burst cluster")
            burst_cursor.execute("xpx 'event set {}'".format(vector.event))

            # Run the failed DML to tombstone the last datablock on burst
            # cluster
            dml_cursor.execute("set query_group to burst;")
            log.info("run dml")
            dml_cmd = self._generate_dml_cmd(vector, burst_table)
            # kDidBurst = 1
            # kFailedNoFailureHandling = 25
            # kFailedNoRerunResultsReturing = 27
            with self.verify_failed_burst_query_status(cluster, vector.dml,
                                                       25):
                dml_cursor.execute_failing_query(dml_cmd,
                                                 "Simulate burst error")
            log.info("unset event on burst cluster")
            burst_cursor.execute("xpx 'event unset {}'".format(vector.event))

            # Refresh the burst cluster and run burst write again to trigger
            # the duplicate tombstone check.
            log.info("refresh and run dml again")
            self._start_and_wait_for_refresh(cluster)
            dml_cursor.execute("set query_group to burst;")
            dml_cursor.execute(dml_cmd)

            log.info("drop table")
            dml_cursor.execute("drop table {};".format(burst_table))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'vacuum_auto_worker_enable': 'false',
        'burst_blk_hdr_stream_threshold': 1,
        'enable_toss_tombstone_blocks_dml': 'false'
    })
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.custom_local_gucs(gucs=burst_write_no_retry_gucs)
class TestBurstWriteDupTombstone(BaseBurstWriteDupTombstone):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even'],
                sortkey=['sortkey(c0)'],
                validate_query_mode=['main'],
                size=[LARGE],
                dml=['copy'],
                event=['EtFakeBurstErrorAfterInsertSnapIn']))

    @pytest.mark.skip(reason="DP-58107")
    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_write_dup_tombstone(self, cluster, db_session, vector):
        assert cluster.get_guc_value(
            'burst_enable_insert_failure_handling') == 'off'

        self.base_test_burst_write_dup_tombstone(cluster, db_session, vector)
