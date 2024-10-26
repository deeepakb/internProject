# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
from threading import Thread
from time import sleep
from raff.burst.burst_test import BurstTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)


def _run_query(db, qtxt):
    try:
        with db.cursor() as cursor:
            cursor.execute(qtxt)
    finally:
        log.info("Done executing query [{}]".format(qtxt))


@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.create_external_schema
@pytest.mark.serial_only
class TestBurstTableName(BurstTest):
    '''This test is to make sure burst or S3 query table name is
       correctly populated in stl_s3query and querying on stv_exec_state
       will not cause sig11 while Spectrum or burst query is running.
    '''

    def start_bg_query(self, cluster, querytxt):
        thread = Thread(
            target=_run_query,
            args=[DbSession(cluster.get_conn_params()), querytxt]
        )
        thread.start()
        log.info('Starting query: [{}]'.format(querytxt))

        return thread

    @pytest.mark.session_ctx(user_type='bootstrap')
    def test_s3_table_name(self, db_session):
        '''This test is to make sure S3 table name logging is not broken.'''
        self.execute_test_file('s3_table_name', session=db_session)

    def test_stv_exec_state_with_spectrum(self, cluster, db_session):
        '''This test is to make sure that querying stv_exec_state will
           not have sig11 when there is running Spectrum queries.'''
        # Set the event to pause the query for a while.
        cluster.set_event('EtSpectrumPauseQuery')

        query = "select * from s3.test_burst_1646_fulltypes_csv"
        # Start a Spectrum query.
        thread = self.start_bg_query(cluster, query)
        # Sleep a while to make sure query is executing.
        # TODO(yichao): replace sleep once rsqa-4030 is fixed.
        sleep(5)

        with self.db.cursor() as cursor:
            try:
                # Run a query on stv_exec_state and this should not
                # cause sig11 while the Spectrum query is running.
                cursor.execute(
                    "select count(*) from  stv_exec_state "
                    "where label like '%test_burst_1646%'"
                )
                num_rows = cursor.fetch_scalar()
                log.info("Found {} records in stv_exec_state".format(num_rows))
                # Check if the table name is logged successfully in
                # stv_exec_state.
                assert num_rows > 0
            finally:
                cluster.unset_event('EtSpectrumPauseQuery')
                thread.join()
