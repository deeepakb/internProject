# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.dimensions import Dimensions
from contextlib import contextmanager

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
DELETE_CMD = "delete from {} where c0 < 3"
UPDATE_CMD = "UPDATE {} set c0 = c0 + 1, c1 = c1 + 1;"
SELECT_CMD = "SELECT * FROM {};"

S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteRestAgentErrors(BurstWriteTest):
    '''
    This class tests random errors on burst and make sure
    cleanup is performed properly by simulating an event
    with random probability.
    It contains two tests, one with writes and one with
    selects only, because the selects don't a refresh
    after an error and can be retried more times.
    '''

    @contextmanager
    def burst_event(self, burst_cursor, error):
        # Set probability of triggering the event to 30%, such that it
        # will be triggered in different phases of the query lifecycle.
        try:
            burst_cursor.execute("xpx 'event set {},prob=30'".format(error))
            yield
        finally:
            burst_cursor.execute("xpx 'event unset {}'".format(error))

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(error=[
                'EtSimulateRestAgentReadRequestError',
                'EtSimulateRestAgentParseMsgSizeError',
                'EtSimulateRestAgentHandleRecvError',
                'EtSimulateRestAgentIncompleteMessageError',
                'EtSimulateRestAgentHandleRecvSidecarError',
                'EtSimulateRestAgentIncompleteSidecarError',
                'EtSimulateRestAgentHandleSendError'
            ]))

    def _setup_tables(self, db_session, burst_table):
        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(burst_table, 'distkey(c0)', 'sortkey(c0)'))
            cursor.execute("begin;")
            cursor.execute(INSERT_CMD.format(burst_table))
            cursor.execute("commit;")

    def _generate_cmd(self, dml_type, tbl_name):
        if dml_type == 'insert':
            return INSERT_CMD.format(tbl_name)
        elif dml_type == 'delete':
            return DELETE_CMD.format(tbl_name)
        elif dml_type == 'update':
            return UPDATE_CMD.format(tbl_name)
        elif dml_type == 'copy':
            return COPY_STMT.format(tbl_name, S3_PATH)
        else:
            return SELECT_CMD.format(tbl_name)

    def test_burst_write_rest_agent_errors(self, cluster, vector, db_session):
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        burst_table = "burst_ra_errors" + str(uuid.uuid4().hex[:8])
        with db_session.cursor() as cur, \
                burst_session.cursor() as burst_cursor:

            log.info("Setting up tables")
            self._setup_tables(db_session, burst_table)
            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)


            # for dml_type in ['insert', 'delete', 'update', 'copy', 'select']:
            for dml_type in ['select']:
                # Try more times such that statistically an error will be triggered.
                retries = 20 if dml_type == 'select' else 3
                for i in range(0, retries):
                    cur.execute("set query_group to burst;")
                    cmd = self._generate_cmd(dml_type, burst_table)
                    try:
                        with self.burst_event(burst_cursor, vector.error):
                            cur.execute(cmd)
                        self.verify_query_bursted(cluster, cur.last_query_id())
                    except Exception as e:
                        log.info(
                            "Query failed because of {} {}. This might be expected".
                            format(type(e).__name__, e))
                        # If a query fails we need to refresh, or the next queries will
                        # not be able to burst because of table ownership reasons.
                        if not dml_type == 'select':
                            self._start_and_wait_for_refresh(cluster)

            burst_cursor.execute(""" SELECT * FROM stv_inflight
                                    WHERE userid > 1""")
            inflight = burst_cursor.fetchall()
            assert len(inflight) == 0, "Inflight queries found: {}".format(
                inflight)
