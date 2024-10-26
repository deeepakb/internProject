# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import getpass
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.storage.alter_table_suite import AlterTableSuite
from raff.common.result import SelectResult
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TABLE = "CREATE TABLE {} (col1 int) diststyle even;"
INSERT = "INSERT INTO {} {} VALUES {};"
SELECT = "SELECT * FROM {};"
MY_TABLE = "test_table"
TARGET_TABLE = "target_table"
RENAME_TABLE = "ALTER TABLE {} RENAME TO {};"
TRUNCATE_TABLE = "TRUNCATE TABLE {};"
APPEND_TABLE = "ALTER TABLE {} APPEND FROM {};"
ADD_COLUMN = "ALTER TABLE {} ADD COLUMN {} {};"
DROP_COLUMN = "ALTER TABLE {} DROP COLUMN {};"
RENAME_COLUMN = "ALTER TABLE {} RENAME COLUMN {} TO {};"


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
class TestBurstQueriesConcurrentRefresh(BurstWriteTest, AlterTableSuite):
    """
    Test that trigger refresh after add column/drop column/alter table
    append/rename(table, column)/truncate, check the burstability of next
    read and write queries, and table content and properties.
    """
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "alter_cmd": [
                "ADD_COLUMN", "DROP_COLUMN", "RENAME_COLUMN", "RENAME_TABLE",
                "TRUNCATE_TABLE", "APPEND_TABLE"
            ]
        })

    def _setup_table(self, cursor):
        cursor.execute(CREATE_TABLE.format(MY_TABLE))
        cursor.execute(INSERT.format(MY_TABLE, "(col1)", "(0), (1)"))

    def _execute_burst_read_query(self, cursor, tablename):
        cursor.execute("set selective_dispatch_level = 0;")
        cursor.execute("set enable_result_cache_for_session to off;")
        cursor.execute(SELECT.format(tablename))

    def _execute_burst_write_query(self, cursor, tablename, columns, values):
        cursor.execute("set selective_dispatch_level = 0;")
        cursor.execute("set enable_result_cache_for_session to off;")
        cols = ','.join(columns)
        cols = "(" + cols + ")"
        vals = ','.join(values)
        vals = "(" + vals + ")"
        log.info("colums are {}".format(cols))
        cursor.execute(INSERT.format(tablename, cols, vals))

    def test_table_actions(self, vector, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_table(cursor)
            if vector.alter_cmd == "ADD_COLUMN":
                cursor.execute("begin;")
                cursor.execute(ADD_COLUMN.format(MY_TABLE, "col2", "INT"))
                cursor.execute(INSERT.format(MY_TABLE, "(col2)", "(0), (1)"))
                cursor.execute("commit;")
                columns = ["col1", "col2"]
                values = ["2", "2"]
                tablename = MY_TABLE
                altered_result = SelectResult(
                    rows=[(0, None), (1, None), (None, 0), (None, 1), (2, 2)],
                    column_types=['INT', 'INT'])

            elif vector.alter_cmd == "DROP_COLUMN":
                cursor.execute("begin;")
                cursor.execute(ADD_COLUMN.format(MY_TABLE, "col2", "INT"))
                cursor.execute(INSERT.format(MY_TABLE, "(col2)", "(0), (1)"))
                cursor.execute(DROP_COLUMN.format(MY_TABLE, "col2"))
                cursor.execute("commit;")
                columns = ["col1"]
                values = ["2"]
                tablename = MY_TABLE
                altered_result = SelectResult(
                    rows=[(0,), (1,), (None,), (None,), (2,)],
                    column_types=['INT'])

            elif vector.alter_cmd == "RENAME_COLUMN":
                cursor.execute("begin;")
                cursor.execute(RENAME_COLUMN.format(
                    MY_TABLE, "col1", "column1"))
                cursor.execute("commit;")
                columns = ["column1"]
                values = ["2"]
                tablename = MY_TABLE
                altered_result = SelectResult(
                    rows=[(0,), (1,), (2,)],
                    column_types=['INT'])

            elif vector.alter_cmd == "RENAME_TABLE":
                cursor.execute("begin;")
                cursor.execute(RENAME_TABLE.format(MY_TABLE, "NEW_TABLE"))
                cursor.execute("commit;")
                columns = ["col1"]
                values = ["2"]
                tablename = "NEW_TABLE"
                altered_result = SelectResult(
                    rows=[(0,), (1,), (2,)],
                    column_types=['INT'])

            elif vector.alter_cmd == "TRUNCATE_TABLE":
                cursor.execute("begin;")
                cursor.execute(TRUNCATE_TABLE.format(MY_TABLE))
                cursor.execute("commit;")
                columns = ["col1"]
                values = ["2"]
                tablename = MY_TABLE
                altered_result = SelectResult(
                    rows=[(2,)],
                    column_types=['INT'])

            # ALTER TABLE APPEND cannot be executed within a transaction
            elif vector.alter_cmd == "APPEND_TABLE":
                cursor.execute(CREATE_TABLE.format(TARGET_TABLE))
                cursor.execute(INSERT.format(TARGET_TABLE, "(col1)", "(3)"))
                cursor.execute(APPEND_TABLE.format(TARGET_TABLE, MY_TABLE))
                tablename = TARGET_TABLE
                columns = ["col1"]
                values = ["2"]
                altered_result = SelectResult(
                    rows=[(3, ), (0, ), (1, ), (2, )], column_types=['INT'])

            # Check Burstability
            self._start_and_wait_for_refresh(cluster)
            self._execute_burst_read_query(cursor, tablename)
            self._check_last_query_bursted(cluster, cursor)
            self._execute_burst_write_query(cursor, tablename, columns, values)
            self._check_last_query_bursted(cluster, cursor)
            self._execute_burst_read_query(cursor, tablename)
            result = cursor.result_fetchall()
            # Check contents
            assert result == altered_result
            # Checking for ownership.
            self._validate_ownership_state(schema, tablename,
                                           [('Burst', 'Owned')])
