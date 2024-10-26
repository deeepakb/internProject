# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

import raff.burst.remote_exec_helpers as helpers
from raff.burst.burst_super_simulated_mode_helper import \
    super_simulated_mode, get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.test_burst_write_mixed_iduc import \
    CREATE_STMT, INSERT_CMD, S3_PATH, COPY_STMT

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

PREFIX = "DP66536"
TABLE_NAME = PREFIX + "_copy_target_tbl"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(
        helpers.burst_unified_remote_exec_gucs_burst(
            slices_per_node=3).items()) + [('udf_start_lxc', 'false')])
@pytest.mark.custom_local_gucs(
    gucs=list(helpers.burst_unified_remote_exec_gucs_main().items()) +
    [('burst_enable_write_copy', 'true'),
     ('enable_burst_failure_handling', 'true')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestUnifiedBurstCopyNoUnnecessaryGraftingSS(BurstWriteTest):
    '''
    Ensure that Unified Burst COPY doesn't do unnecessary grafting.
    Specifically, the test covers the following scenarios:
    - COPY scheduled and executed on Main:
        - Ensure no grafting happened at all (neither at Main nor Burst).
    - COPY scheduled for Burst but failed there b/c Burst has no available slot:
        - Ensure no grafting happened at all (neither at Main nor Burst).
    - COPY scheduled and executed on Burst:
        - Ensure grafting happened on Burst only.
    '''

    def test_unified_burst_copy_no_unnecessary_grafting_ss(self, cluster):
        with DbSession(cluster.get_conn_params()) as session, session.cursor(
        ) as cursor, self.db.cursor() as syscur, RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            schema = self._schema(cursor)
            table_name = schema + "." + TABLE_NAME
            cursor.execute("set query_group to metrics")

            cursor.execute("BEGIN")
            cursor.execute(CREATE_STMT.format("", TABLE_NAME, "", ""))
            cursor.execute(INSERT_CMD.format(TABLE_NAME))
            cursor.execute("COMMIT")

            # COPY scheduled and executed on Main:
            cursor.execute("BEGIN")
            cursor.execute(COPY_STMT.format(TABLE_NAME, S3_PATH))
            self._check_last_copy_didnt_burst(cluster, cursor)
            grafted_on_main = self._txn_performed_grafting(
                syscur, self._txn_id(cursor), table_name)
            grafted_on_burst = self._txn_performed_grafting(
                burst_cursor, self._txn_id(cursor), table_name)
            assert not grafted_on_main, \
                "COPY on Main did unnecessary grafting on Main."
            assert not grafted_on_burst, \
                "COPY on Main did unnecessary grafting on Burst."
            cursor.execute("COMMIT")

            # COPY scheduled on Burst but rejected there due to a queueing error:
            cursor.execute("set query_group to burst")
            cursor.execute("BEGIN")
            burst_cursor.execute("xpx 'event set EtWlmBurstRouteFailure'")
            try:
                cursor.execute(COPY_STMT.format(TABLE_NAME, S3_PATH))
            finally:
                burst_cursor.execute(
                    "xpx 'event unset EtWlmBurstRouteFailure'")
            self._check_last_copy_didnt_burst(cluster, cursor)
            grafted_on_main = self._txn_performed_grafting(
                syscur, self._txn_id(cursor), table_name)
            grafted_on_burst = self._txn_performed_grafting(
                burst_cursor, self._txn_id(cursor), table_name)
            assert not grafted_on_main, \
                "Rejected COPY on Burst did unnecessary grafting on Main."
            assert not grafted_on_burst, \
                "Rejected COPY on Burst did unnecessary grafting on Burst."
            cursor.execute("COMMIT")

            # COPY executed on Burst successfully:
            cursor.execute("BEGIN")
            cursor.execute(COPY_STMT.format(TABLE_NAME, S3_PATH))
            self._check_last_copy_bursted(cluster, cursor)
            grafted_on_main = self._txn_performed_grafting(
                syscur, self._txn_id(cursor), table_name)
            grafted_on_burst = self._txn_performed_grafting(
                burst_cursor, self._txn_id(cursor), table_name)
            assert not grafted_on_main, \
                "COPY on Burst did unnecessary grafting on Main."
            assert grafted_on_burst, \
                "COPY on Burst did not graft on Burst."
            cursor.execute("COMMIT")

    def _schema(self, cursor):
        cursor.execute("SELECT current_schema()")
        return cursor.fetchall()[0][0].strip()

    def _txn_id(self, cursor):
        cursor.execute("SELECT txid_current()")
        return cursor.fetch_scalar()

    def _txn_performed_grafting(self, syscur, txn, table):
        syscur.execute("SELECT count(*) FROM stl_datashare_query_info "
                       "WHERE xid = {} AND "
                       "schema_name_table_name ilike '%{}%'".format(
                           txn, table))
        num_grafts = syscur.fetch_scalar()
        return num_grafts != 0
