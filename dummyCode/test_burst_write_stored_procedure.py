# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import random

from six.moves import range
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.common.host_type import HostType
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

DROP_TABLE = "DROP TABLE IF EXISTS public.{}{}{};"
CREATE_STMT = "CREATE TABLE public.{}{}{} (c0 int, c1 int) {} {};"
CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")
GRANT_STMT = "GRANT ALL ON public.{}{}{} TO PUBLIC;"
INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {};"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10);"
DELETE_CMD = "delete from {} where c0 < 3;"
UPDATE_CMD_1 = "update {} set c0 = c0*2 where c0 > 5;"
UPDATE_CMD_2 = "update {} set c0 = c0 + 2;"
S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")
PROCEDURE_STMT = """
CREATE OR REPLACE PROCEDURE burst_write_procedure()
AS $$
BEGIN
{}
END;
$$ LANGUAGE plpgsql;
"""


class TestBurstWriteStoredProcedureBase(BurstTempWrite):
    def _setup_tables(self, cluster, cursor, base_tbl_name):
        diststyles=['diststyle even', 'distkey(c0)']
        sortkeys=['', 'sortkey(c0)']
        tbl_index = 0
        base_tbl_list = []
        for diststyle in diststyles:
            for sortkey in sortkeys:
                self._run_bootstrap_sql(cluster, DROP_TABLE.format(
                    base_tbl_name, "_not_burst_", tbl_index))
                self._run_bootstrap_sql(cluster, DROP_TABLE.format(
                    base_tbl_name, "_burst_", tbl_index))
                cursor.execute(CREATE_STMT.format(
                    base_tbl_name, "_not_burst_", tbl_index, diststyle,
                    sortkey))
                cursor.execute(CREATE_STMT.format(
                    base_tbl_name, "_burst_", tbl_index, diststyle, sortkey))
                cursor.execute(GRANT_STMT.format(
                    base_tbl_name, "_not_burst_", tbl_index))
                cursor.execute(GRANT_STMT.format(
                    base_tbl_name, "_burst_", tbl_index))
                base_tbl_list.append("{}{}{}".format(base_tbl_name,
                                                     "_not_burst_", tbl_index))
                base_tbl_list.append("{}{}{}".format(base_tbl_name, "_burst_",
                                                     tbl_index))
                tbl_index = tbl_index + 1
        return tbl_index, base_tbl_list

    def _setup_clone_temp_tables_with_suffix(self, cursor, schema,
                                             to_clone_table_list):
        """
        Setup temp tables cloned from to_clone_table_list.

        Args:
            cursor: main cluster cursor
            schema: schema of tables in to_clone_table_list
            to_clone_table_list: list of table names
        """
        temp_table_dict = {}
        for table in to_clone_table_list:
            log.info("Creating temp table clone for: {}.{}".format(
                schema, table))
            clone_temp_table = table + "_temp" + str(random.randint(1, 9999))
            cursor.execute(
                CREATE_TEMPORARY_TABLE_SQL.format(clone_temp_table, schema,
                                                  table))
            temp_tbl_schema = \
                self._get_temp_table_schema(cursor, clone_temp_table)
            temp_table_info = []
            temp_table_info.append(temp_tbl_schema)
            temp_table_info.append(clone_temp_table)
            temp_table_dict[table] = temp_table_info
            log.info(
                "Temp table name: {}, temp table schema: {}, base_table: {}".
                format(clone_temp_table, temp_tbl_schema, table))
        return temp_table_dict

    def run_query_in_sp(self, cluster, cursor, query, is_burst):
        if is_burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        cursor.execute(PROCEDURE_STMT.format(query))
        cursor.execute("CALL burst_write_procedure();")
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("DROP PROCEDURE burst_write_procedure();")

    def _insert_tables(self, cluster, cursor, tbl_name_noburst,
                       tbl_name_burst):
        # Insert when set not bursted.
        query = INSERT_CMD.format(tbl_name_noburst)
        self.run_query_in_sp(cluster, cursor, query, False)
        # Insert when set bursted.
        query = INSERT_CMD.format(tbl_name_burst)
        self.run_query_in_sp(cluster, cursor, query, True)

    def _insert_select_tables(self,
                              cluster,
                              cursor,
                              tbl_name_noburst,
                              tbl_name_burst,
                              num=1):
        for i in range(num):
            # Insert when set not bursted.
            query = INSERT_SELECT_CMD.format(tbl_name_noburst,
                                             tbl_name_noburst)
            self.run_query_in_sp(cluster, cursor, query, False)
            # Insert when set bursted.
            query = INSERT_SELECT_CMD.format(tbl_name_burst, tbl_name_burst)
            self.run_query_in_sp(cluster, cursor, query, True)

    def _delete_tables(self, cluster, cursor, tbl_name_noburst,
                       tbl_name_burst):
        # Delete when set not bursted.
        query = DELETE_CMD.format(tbl_name_noburst)
        self.run_query_in_sp(cluster, cursor, query, False)
        # Delete when set bursted.
        query = DELETE_CMD.format(tbl_name_burst)
        self.run_query_in_sp(cluster, cursor, query, True)

    def _update_tables_1(self, cluster, cursor, tbl_name_noburst,
                         tbl_name_burst):
        # Update when set not bursted.
        query = UPDATE_CMD_1.format(tbl_name_noburst)
        self.run_query_in_sp(cluster, cursor, query, False)
        # Update when set bursted.
        query = UPDATE_CMD_1.format(tbl_name_burst)
        self.run_query_in_sp(cluster, cursor, query, True)

    def _update_tables_2(self, cluster, cursor, tbl_name_noburst,
                         tbl_name_burst):
        # Update when set not bursted.
        query = UPDATE_CMD_2.format(tbl_name_noburst)
        self.run_query_in_sp(cluster, cursor, query, False)
        # Update when set bursted.
        query = UPDATE_CMD_2.format(tbl_name_burst)
        self.run_query_in_sp(cluster, cursor, query, True)

    def _copy_tables(self, cluster, cursor, tbl_name_noburst, tbl_name_burst):
        # Copy when set not bursted.
        query = COPY_STMT.format(tbl_name_noburst, S3_PATH)
        self.run_query_in_sp(cluster, cursor, query, False)
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_query_didnt_burst(cluster, cursor)
        # Copy when set bursted.
        query = COPY_STMT.format(tbl_name_burst, S3_PATH)
        self.run_query_in_sp(cluster, cursor, query, True)
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_copy_bursted(cluster, cursor)

    def base_test_burst_write_sp(self, cluster, txn_mode, is_temp):
        """
        Test: test stored procedure can be burst write correctly.
        The test includes:
        1. Run insert/delete/update/copy within a stored procedure on the burst
           culster.
        2. Validate tables content at last.
        """
        cluster.run_xpx('auto_worker disable both')
        base_tbl_name = "burst_write_sp"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            base_table_schema = "public"
            num_tbls, base_tbl_list = self._setup_tables(
                cluster, cursor, base_tbl_name)
            if is_temp:
                temp_table_dict = self._setup_clone_temp_tables_with_suffix(
                    cursor, base_table_schema, base_tbl_list)
            self._start_and_wait_for_refresh(cluster)
            if txn_mode == "commit" or "abort":
                cursor.execute("BEGIN;")
            for tbl_index in range(num_tbls):
                tbl_name_noburst = base_tbl_list[2 * tbl_index]
                tbl_name_burst = base_tbl_list[2 * tbl_index + 1]
                if is_temp:
                    tbl_name_noburst = "{}.{}".format(
                        temp_table_dict.get(tbl_name_noburst)[0],
                        temp_table_dict.get(tbl_name_noburst)[1])
                    tbl_name_burst = "{}.{}".format(
                        temp_table_dict.get(tbl_name_burst)[0],
                        temp_table_dict.get(tbl_name_burst)[1])
                self._insert_tables(cluster, cursor, tbl_name_noburst,
                                    tbl_name_burst)
                self._insert_select_tables(cluster, cursor, tbl_name_noburst,
                                           tbl_name_burst, 10)
                self._delete_tables(cluster, cursor, tbl_name_noburst,
                                    tbl_name_burst)
                self._update_tables_1(cluster, cursor, tbl_name_noburst,
                                      tbl_name_burst)
                self._update_tables_2(cluster, cursor, tbl_name_noburst,
                                      tbl_name_burst)
            if txn_mode == "abort":
                cursor.execute("ABORT;")
            else:
                cursor.execute("COMMIT;")
            if not is_temp:
                self._start_and_wait_for_refresh(cluster)
            for tbl_index in range(num_tbls):
                tbl_name_noburst = base_tbl_list[2 * tbl_index]
                tbl_name_burst = base_tbl_list[2 * tbl_index + 1]
                if is_temp:
                    tbl_name_noburst = "{}.{}".format(
                        temp_table_dict.get(tbl_name_noburst)[0],
                        temp_table_dict.get(tbl_name_noburst)[1])
                    tbl_name_burst = "{}.{}".format(
                        temp_table_dict.get(tbl_name_burst)[0],
                        temp_table_dict.get(tbl_name_burst)[1])
                if txn_mode != 'abort':
                    self._copy_tables(cluster, cursor, tbl_name_noburst,
                                      tbl_name_burst)
                self._validate_content_equivalence(
                    cluster, cursor, tbl_name_noburst, tbl_name_burst)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
class TestBurstWriteStoredProcedureCluster(TestBurstWriteStoredProcedureBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(txn_mode=["no_txn", "abort", "commit"]))

    @pytest.mark.parametrize("is_temp", [True, False])
    def test_burst_write_sp_cluster(self, cluster, vector, is_temp):
        self.base_test_burst_write_sp(cluster, vector.txn_mode, is_temp)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.ssm_perm_or_temp_config
class TestBurstWriteStoredProcedureSS(TestBurstWriteStoredProcedureBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(txn_mode=["no_txn", "abort", "commit"]))

    def test_burst_write_sp_ss(self, cluster, vector, is_temp):
        self.base_test_burst_write_sp(cluster, vector.txn_mode, is_temp)
