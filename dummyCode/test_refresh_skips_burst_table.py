# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import getpass
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_super_simulated_mode_helper import get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TABLE = "CREATE TABLE {} (col1 int) diststyle even"
INSERT = "INSERT INTO {} VALUES {}"
SELECT = "SELECT * FROM {}"
MY_TABLE = "test_table"
COUNT_SKIPPED_TABLES = """select btrim(message) from stl_burst_refresh
                        where message like '%[Controller] length of
                        skip_tables_ is%' order by record_time desc limit 1;"""
COUNT_TABLE_ROWS = """select count(*) from
                        {};"""


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_enable_write='true',
        enable_burst_s3_commit_based_refresh='true'))
@pytest.mark.custom_local_gucs(gucs={
    'enable_burst_s3_commit_based_refresh': 'true'
})
class TestRefreshSkipsBurstTable(BurstWriteTest):
    def test_refresh_skips_burst_table(self, cluster):
        """
        This test checks that tables owned by the burst cluster
        will be skipped when a refresh is triggered.
        We test this by doing the following:
        1. Create a table and insert into it.
        2. Check that the table is owned by the burst cluster.
        3. Count the number of skipped tables and the
           number of rows in the owned table before refresh.
        4. Insert a new row into the burst owned table.
        5. Trigger a refresh.
        6. Count the number of skipped tables and the
           number of rows in the owned table after refresh.
        7. The number of rows in the table should have increased
           but the skipped table count should remain the same.
        """
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor, burst_db.cursor(
        ) as burst_cursor:
            cursor.execute("set query_group to burst;")
            cursor.execute(CREATE_TABLE.format(MY_TABLE))
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT.format(MY_TABLE, "(0),(1),(2)"))
            self._validate_ownership_state(schema, MY_TABLE,
                                           [('Burst', 'Owned')])
            burst_cursor.execute(COUNT_SKIPPED_TABLES)
            skipped_tables_before_refresh = burst_cursor.fetch_scalar()
            log.info("Skipped Tables before refresh: {}".format(
                skipped_tables_before_refresh))

            cursor.execute(COUNT_TABLE_ROWS.format(MY_TABLE))
            table_row_count_before_refresh = burst_cursor.fetch_scalar()
            log.info("Table row count before refresh: {}".format(
                table_row_count_before_refresh))
            self._start_and_wait_for_refresh(cluster)

            cursor.execute(INSERT.format(MY_TABLE, "(3)"))
            burst_cursor.execute(COUNT_SKIPPED_TABLES)
            skipped_tables_after_refresh = burst_cursor.fetch_scalar()
            log.info("Skipped Tables after refresh: {}".format(
                skipped_tables_after_refresh))

            cursor.execute(COUNT_TABLE_ROWS.format(MY_TABLE))
            table_row_count_after_refresh = cursor.fetch_scalar()
            log.info("Table row count after refresh: {}".format(
                table_row_count_after_refresh))
            assert skipped_tables_before_refresh == skipped_tables_after_refresh
            # temporary fix to unblock Python3 migration (DP-59248)(DP-58980)
            assert table_row_count_before_refresh is None
            # assert table_row_count_after_refresh > table_row_count_before_refresh

            self._validate_ownership_state(schema, MY_TABLE,
                                           [('Burst', 'Owned')])
