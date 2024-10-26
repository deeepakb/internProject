# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

log = logging.getLogger(__name__)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")
RENAME_TABLE = "ALTER TABLE {} RENAME TO {};"


@pytest.mark.load_tpcds_data
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstPreparedStatement(BurstTempWrite):
    def _setup_clone_temp_tables(self, cursor, schema, to_clone_table_list):
        for table in to_clone_table_list:
            log.info("Creating temp table clone for: {}.{}".format(
                schema, table))
            base_table = table + "_base"
            cursor.execute(
                CREATE_TEMPORARY_TABLE_SQL.format(table, schema, base_table))

    def run_test(self, db_session, is_temp):
        if (is_temp):
            # Set up testing tables and backup.
            to_clone_table_list = [
                "store", "store_sales", "customer", "date_dim"
            ]
            base_schema = "public"
            with db_session.cursor() as cursor:
                # Alter to be cloned table names to "<table>_base"
                for table in to_clone_table_list:
                    base_table_name = table + "_base"
                    cursor.execute(RENAME_TABLE.format(table, base_table_name))
                # Create temp table by cloning base table, and temp tables
                # have same name as orignal base table name
                self._setup_clone_temp_tables(cursor, base_schema,
                                              to_clone_table_list)
                self.execute_test_file(
                    'test_burst_prepared_statement', session=db_session)
                # Revert base table name back to in order to reuse the codes
                # above in repeated tests
                for table in to_clone_table_list:
                    changed_table_name = table + "_base"
                    cursor.execute(
                        RENAME_TABLE.format(changed_table_name, table))

        else:
            self.execute_test_file(
                'test_burst_prepared_statement', session=db_session)

    def test_burst_prepared_statement(self, db_session, is_temp):
        """
        Tests various prepared statements on burst using different combination
        of parameter types and number of parameters.
        """
        self.run_test(db_session, is_temp)
