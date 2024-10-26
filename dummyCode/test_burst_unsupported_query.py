# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
"""
This file is used to test table ownership with unsupported queries. It provides
a framework to simplify writing test cases.
"""

import logging

import pytest

from raff.burst.burst_status import BurstStatus
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write_unsupported_query import \
    BurstWriteUnsupportedQueryBase, BWUnsupportedQuerySystemTable, \
    BWUnsupportedQuerySimpleBase
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)


@pytest.mark.skip_load_data
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWUnsupportedQuerySpectrumCopy(BurstWriteUnsupportedQueryBase):
    """
    This is the test case for spectrum copy.
    """

    _TABLE_NAME = "bw_unsupported_spectrum_copy"
    _CREATE_TABLE_QUERY = ("CREATE TABLE {} ("
                           "  csmallint        INT,"
                           "  cint             INT,"
                           "  cbigint          BIGINT,"
                           "  cfloat           FLOAT4,"
                           "  cdouble          FLOAT8,"
                           "  cchar            CHAR(10),"
                           "  cvarchar         VARCHAR(255),"
                           "  cdecimal_small   DECIMAL(18,9),"
                           "  cdecimal_big     DECIMAL(30,15),"
                           "  ctimestamp       TIMESTAMP,"
                           "  cboolean         BOOLEAN,"
                           "  cstring          VARCHAR(255)"
                           ") DISTSTYLE even")
    _SPECTRUM_COPY_QUERY = ("COPY {} "
                            "FROM 's3://dory-data/alldatatypes_parquet/' "
                            "IAM_ROLE "
                            "'arn:aws:iam::467896856988:role/Redshift-S3' "
                            "FORMAT AS parquet")
    _TOTAL_NUM_SLICES_QUERY = ("SELECT COUNT(slice) FROM stv_slices "
                               "WHERE type = 'D'")
    _NUM_INSERTED_SLICES_QUERY = ("SELECT COUNT(DISTINCT slice) "
                                  "FROM stv_blocklist "
                                  "WHERE tbl = (SELECT oid FROM pg_class "
                                  "             WHERE relname = '{}') "
                                  "             AND col = 10")
    _INSERT_QUERY = ("INSERT INTO {} VALUES (1,1,1,1,1,'bintian','bintian',1,"
                     "1,'2022-01-01', true, 'bintian')")

    def _get_table_name(self):
        return self._TABLE_NAME

    def _get_unsupported_status_code(self):
        return BurstStatus.burst_spectrum_copy

    def _prepare_data(self, worker_cursor):
        log.info("Dropping test table if exists")
        worker_cursor.execute(self._DROP_TABLE_QUERY.format(self._TABLE_NAME))
        log.info("Creating test table")
        worker_cursor.execute(
            self._CREATE_TABLE_QUERY.format(self._TABLE_NAME))
        worker_cursor.execute(self._TOTAL_NUM_SLICES_QUERY)
        num_slices = worker_cursor.fetch_scalar()
        log.info("Total number of slices: {}".format(num_slices))
        num_inserted_slices = 0
        with self.db.cursor() as status_cursor:
            while num_inserted_slices < num_slices:
                worker_cursor.execute(
                    self._SPECTRUM_COPY_QUERY.format(self._TABLE_NAME))
                status_cursor.execute(
                    self._NUM_INSERTED_SLICES_QUERY.format(self._TABLE_NAME))
                num_inserted_slices = status_cursor.fetch_scalar()
                log.info("Number of inserted slices: {}".format(
                    num_inserted_slices))

    def _run_query_to_own_burst(self, worker_cursor):
        self._switch_to_burst_queue(worker_cursor)
        worker_cursor.execute(self._INSERT_QUERY.format(self._TABLE_NAME))
        return self.last_query_id(worker_cursor)

    def _run_unsupported_query(self, worker_cursor):
        self._switch_to_burst_queue(worker_cursor)
        worker_cursor.execute(
            self._SPECTRUM_COPY_QUERY.format(self._TABLE_NAME))
        return self.last_query_id(worker_cursor)

    def test_spectrum_copy(self, cluster):
        """
        The main function of this test case.

        Args:
            cluster: The cluster to run test.
        """
        self._run_test(cluster)


class TestBWUnsupportedQueryStlSystemTable(BWUnsupportedQuerySystemTable):
    """
    This is the test case for stl system table.
    """

    _STL_TABLE_NAME = "bw_unsupported_stl_system_table"
    _STL_SYSTEM_TABLE_QUERY = "INSERT INTO {} SELECT query FROM stl_query"

    def _get_query_text(self):
        return self._STL_SYSTEM_TABLE_QUERY

    def _get_table_name(self):
        return self._STL_TABLE_NAME


class TestBWUnsupportedQueryStvSystemTable(BWUnsupportedQuerySystemTable):
    """
    This is the test case for stv system table.
    """

    _STV_TABLE_NAME = "bw_unsupported_stv_system_table"
    _STV_SYSTEM_TABLE_QUERY = "INSERT INTO {} SELECT slice FROM stv_slices"

    def _get_query_text(self):
        return self._STV_SYSTEM_TABLE_QUERY

    def _get_table_name(self):
        return self._STV_TABLE_NAME


class TestBWUnsupportedQueryPgTable(BWUnsupportedQuerySimpleBase):
    """
    This is the test case for pg table.
    """

    _TABLE_NAME = "bw_unsupported_pg_table"
    _PG_TABLE_QUERY = ("INSERT INTO {} SELECT oid FROM pg_class "
                       "WHERE relname = '{}'")

    def _get_query_text(self):
        return self._PG_TABLE_QUERY.format(self._TABLE_NAME, self._TABLE_NAME)

    def _get_table_name(self):
        return self._TABLE_NAME

    def _get_unsupported_status_code(self):
        return BurstStatus.pg_tables_accessed


@pytest.mark.skip_load_data
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_local_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_user_temp_table': 'false'
})
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true',
    'burst_enable_user_temp_table': 'false'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWUnsupportedQueryTempTable(BurstWriteUnsupportedQueryBase):

    _PERM_TABLE_NAME = "bw_perm_table"
    _TEMP_TABLE_NAME = "bw_temp_table"
    _PERM_TABLE_CREATE = "CREATE TABLE {} (a INT) DISTSTYLE EVEN"
    _TEMP_TABLE_CREATE = "CREATE TEMP TABLE {} (a INT) DISTSTYLE EVEN"
    _SIMPLE_TABLE_INSERT = "INSERT INTO {} VALUES (1)"
    _TEMP_TO_PERM_TABLE_INSERT = "INSERT INTO {} SELECT * from {}"

    def _get_table_name(self):
        """
        The interface used to get the name of test table.
        """
        return self._PERM_TABLE_NAME

    def _get_unsupported_status_code(self):
        """
        The interface used to get the burst status code.
        """
        return BurstStatus.user_temp_tables_acessed

    def _prepare_data(self, worker_cursor):
        """
        The interface used to load data before running the unsupported query.

        Args:
            worker_cursor: The main cursor used to run required queries in each
                           test
        """
        log.info("Dropping test tables if exists")
        worker_cursor.execute(
            self._DROP_TABLE_QUERY.format(self._PERM_TABLE_NAME))
        worker_cursor.execute(
            self._DROP_TABLE_QUERY.format(self._TEMP_TABLE_NAME))
        log.info("Creating test tables")
        worker_cursor.execute(
            self._PERM_TABLE_CREATE.format(self._PERM_TABLE_NAME))
        worker_cursor.execute(
            self._TEMP_TABLE_CREATE.format(self._TEMP_TABLE_NAME))
        log.info("Inserting into test tables")
        worker_cursor.execute(
            self._SIMPLE_TABLE_INSERT.format(self._PERM_TABLE_NAME))
        worker_cursor.execute(
            self._SIMPLE_TABLE_INSERT.format(self._TEMP_TABLE_NAME))

    def _run_unsupported_query(self, worker_cursor):
        """
        The interface used to run the unsupported query.

        Args:
            worker_cursor: The main cursor used to run required queries in each
            test.

        Returns:
            The query id of the unsupported query.
        """
        self._switch_to_burst_queue(worker_cursor)
        worker_cursor.execute(
            self._TEMP_TO_PERM_TABLE_INSERT.format(self._PERM_TABLE_NAME,
                                                   self._TEMP_TABLE_NAME))
        return self.last_query_id(worker_cursor)

    def _run_query_to_own_burst(self, worker_cursor):
        """
        The interface used to run a burst eligible query to own burst cluster.

        Args:
            worker_cursor: The main cursor used to run required queries in each
            test.

        Returns:
            The query id of the burst eligible query.
        """
        self._switch_to_burst_queue(worker_cursor)
        worker_cursor.execute(
            self._SIMPLE_TABLE_INSERT.format(self._PERM_TABLE_NAME))
        return self.last_query_id(worker_cursor)

    def test_insert_from_temp_table_to_perm_table(self, cluster):
        """
        The main function of this test case. It does the following:
        1. create a temp table bw_temp_table and insert a row.
        2. create a perm table bw_perm_table and insert a row.
        3. insert into bw_perm_table (run it on burst). Burst is now the owner
            of it.
        4. insert from bw_temp_table to bw_perm_table. The query should have
            the status of user_temp_tables_acessed, and main is now the owner
            of bw_perm_table and bw_temp_table.
        5. select from bw_perm_table. Main is the owner, and it should run on
            main, even tho it can run on burst.

        Args:
            cluster: The cluster to run test.
        """
        self._run_test(cluster)


@pytest.mark.skip_load_data
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'false'
    })
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_user_ctas': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWUnsupportedQueryCTAS(BurstWriteTest):
    """
    This is the test case for CTAS.
    1. create a table bw_table.
    2. Insert into bw_table (run it on burst. burst is now the owner)
    3. Run a CTAS query:
        create table bw_unsupported_ctas as select * from bw_table. (main is
        now the owner of bw_unsupported_ctas, burst is the owner of bw_table,
        and the status of this query should be kBurstWriteForUserCTASDisabled)
    4. insert into bw_unsupported_ctas (main is the owner, and it should run on
        main, even tho it can run on burst)
    5. insert into bw_table (burst is the owner)
    """

    _NORMAL_TABLE_NAME = "bw_table"
    _CTAS_TABLE_NAME = "bw_unsupported_ctas"
    _SIMPLE_TABLE_CREATE = "CREATE TABLE {} (a INT) DISTSTYLE EVEN"
    _CTAS_TABLE_CREATE = "CREATE TABLE {} DISTSTYLE EVEN AS SELECT * FROM {};"
    _DROP_TABLE_QUERY = "DROP TABLE IF EXISTS {}"
    _SIMPLE_TABLE_INSERT = "INSERT INTO {} VALUES (1)"
    _SWITCH_TO_BURST_QUEUE_QUERY = "SET query_group TO burst"
    _GET_QID_FROM_QUERY = ("SELECT query FROM stl_query WHERE "
                           "UPPER(BTRIM(querytxt)) = UPPER(BTRIM('{}'))")

    def test_table_ownership_for_ctas(self, cluster):
        with DbSession(cluster.get_conn_params()) as db_session:
            cursor = db_session.cursor()
            schema = db_session.session_ctx.schema
            self._create_normal_table(cursor)
            self._start_and_wait_for_refresh(cluster)

            cursor.execute(self._SWITCH_TO_BURST_QUEUE_QUERY)
            cursor.execute(
                self._SIMPLE_TABLE_INSERT.format(self._NORMAL_TABLE_NAME))
            qid = self.last_query_id(cursor)
            self._validate_query_burst_status(qid, BurstStatus.did_burst)
            self._validate_ownership(
                schema,
                self._NORMAL_TABLE_NAME,
                True,  # is_owned_by_burst
                self._get_query_xid(qid))

            self._create_ctas_table(cursor)
            qid = self._get_qid_for_create_ctas_table_query(cursor)
            self._validate_query_burst_status(
                qid, BurstStatus.burst_write_for_user_ctas_disabled)

            self._validate_ownership(
                schema,
                self._NORMAL_TABLE_NAME,
                True  # is_owned_by_burst
            )
            self._validate_ownership(
                schema,
                self._CTAS_TABLE_NAME,
                False  # is_owned_by_burst
            )

            cursor.execute(
                self._SIMPLE_TABLE_INSERT.format(self._CTAS_TABLE_NAME))
            self._validate_ownership(
                schema,
                self._CTAS_TABLE_NAME,
                False  # is_owned_by_burst
            )

            cursor.execute(
                self._SIMPLE_TABLE_INSERT.format(self._NORMAL_TABLE_NAME))
            qid = self.last_query_id(cursor)
            self._validate_query_burst_status(qid, BurstStatus.did_burst)
            self._validate_ownership(
                schema,
                self._NORMAL_TABLE_NAME,
                True  # is_owned_by_burst
            )

    def _create_normal_table(self, worker_cursor):
        log.info("Dropping test table if exists")
        worker_cursor.execute(
            self._DROP_TABLE_QUERY.format(self._NORMAL_TABLE_NAME))
        log.info("Creating test table")
        worker_cursor.execute(
            self._SIMPLE_TABLE_CREATE.format(self._NORMAL_TABLE_NAME))

    def _get_create_ctas_table_query_statement(self):
        return self._CTAS_TABLE_CREATE.format(self._CTAS_TABLE_NAME,
                                              self._NORMAL_TABLE_NAME)

    def _create_ctas_table(self, worker_cursor):
        log.info("Dropping ctas table if exists")
        worker_cursor.execute(
            self._DROP_TABLE_QUERY.format(self._CTAS_TABLE_NAME))
        log.info("Creating ctas table")
        worker_cursor.execute(self._get_create_ctas_table_query_statement())

    def _get_qid_for_create_ctas_table_query(self, worker_cursor):
        worker_cursor.execute(
            self._GET_QID_FROM_QUERY.format(
                self._get_create_ctas_table_query_statement()))
        qid = worker_cursor.fetch_scalar()
        log.info("qid for create ctas table query: %s" % qid)
        return qid


@pytest.mark.skip_load_data
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWUnsupportedQueryShadowTable(BurstWriteTest):

    _TABLE_NAME_FOR_COPY = "bw_unsupported_shadow_copy"
    _TABLE_NAME_FOR_INSERT = "bw_unsupported_shadow_insert"
    _TABLE_CREATE = "CREATE TABLE {} (c0 INT, c1 INT) DISTSTYLE EVEN;"
    _CHANGE_DIST_KEY = "ALTER TABLE {} ALTER DISTKEY c0;"
    _TABLE_SINGLE_ROW_INSERT = "INSERT INTO {} VALUES (1, 1);"
    _DROP_TABLE_QUERY = "DROP TABLE IF EXISTS {}"
    _SWITCH_TO_BURST_QUEUE_QUERY = "SET query_group TO burst"
    _S3_PATH = \
        's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
    _COPY_STMT = ("COPY {} (c0, c1) "
                  "FROM "
                  "'{}' "
                  "DELIMITER ',' "
                  "CREDENTIALS "
                  "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")

    def _get_copy_statement(self, tbl_name):
        return self._COPY_STMT.format(tbl_name, self._S3_PATH)

    def _copy_tables(self, cursor, tbl_name):
        cursor.execute(self._get_copy_statement(tbl_name))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21

    def _prepare_data(self, cursor, tbl_name):
        """
        Create the test table before running the unsupported query.

        Args:
            cursor: The main cursor used to run required queries.
        """
        log.info("Dropping test table if exists")
        cursor.execute(self._DROP_TABLE_QUERY.format(tbl_name))
        log.info("Creating test table")
        cursor.execute(self._TABLE_CREATE.format(tbl_name))

    def _run_query_to_own_burst(self, cursor, tbl_name):
        """
        Run a burst eligible query to own burst cluster.

        Args:
            cursor: The main cursor used to run required queries.

        Returns:
            The query id of the burst eligible query.
        """
        cursor.execute(self._SWITCH_TO_BURST_QUEUE_QUERY)
        cursor.execute(self._TABLE_SINGLE_ROW_INSERT.format(tbl_name))

        return self.last_query_id(cursor)

    def _copy_data_to_shadow_table(self, cursor, tbl_name):
        """
        Steps:
        1. Begin a transaction.
        2. Change distkey on the test table. This will create a shadow table
            for the test table in the transaction.
        3. copy data from s3 to the test table.
        4. Commit

        Args:
            cursor: The cursor to run queries.

        Returns:
            The query id of the copy query.
        """

        cursor.execute("BEGIN;")
        cursor.execute(self._CHANGE_DIST_KEY.format(tbl_name))
        self._copy_tables(cursor, tbl_name)
        qid = self.last_query_id(cursor)
        cursor.execute("COMMIT;")
        return qid

    def test_copy_into_shadow_table(self, cluster):
        """
        The main function of this test case. It does the following:
        1. Create a test table.
        2. Take a backup and refresh burst cluster.
        3. Insert into the test table. It should run on burst and owned by
            burst.
        4. Begin a transaction. Change distkey on the test table to make it a
            shadow table. And then copy data to the test table, and commit.
        5. Now the test table should be owned by main, and the copy query
            should have write_query_on_tbl_with_shadow_table 49 status.

        Args:
            cluster: The cluster to run test.
        """

        with DbSession(cluster.get_conn_params()) as db_session:
            cursor = db_session.cursor()
            schema = db_session.session_ctx.schema
            self._prepare_data(cursor, self._TABLE_NAME_FOR_COPY)
            self._start_and_wait_for_refresh(cluster)

            qid = self._run_query_to_own_burst(cursor,
                                               self._TABLE_NAME_FOR_COPY)

            self._validate_query_burst_status(qid, BurstStatus.did_burst)
            self._validate_ownership(
                schema,
                self._TABLE_NAME_FOR_COPY,
                True,  # is_owned_by_burst
                self._get_query_xid(qid))

            qid = self._copy_data_to_shadow_table(cursor,
                                                  self._TABLE_NAME_FOR_COPY)

            self._validate_query_burst_status(
                qid, BurstStatus.write_query_on_tbl_with_shadow_table)
            self._validate_ownership(
                schema,
                self._TABLE_NAME_FOR_COPY,
                False  # is_owned_by_burst
            )

    def _insert_into_shadow_table(self, cursor, tbl_name):
        """
        Steps:
        1. Begin a transaction.
        2. Change distkey on the test table. This will create a shadow table
            for the test table in the transaction.
        3. insert data into the test table.
        4. Commit

        Args:
            cursor: The cursor to run queries.

        Returns:
            The query id of the copy query.
        """

        cursor.execute("BEGIN;")
        cursor.execute(self._CHANGE_DIST_KEY.format(tbl_name))
        cursor.execute(self._TABLE_SINGLE_ROW_INSERT.format(tbl_name))
        qid = self.last_query_id(cursor)
        cursor.execute("COMMIT;")
        return qid

    def test_insert_into_shadow_table(self, cluster):
        """
        The main function of this test case. It does the following:
        1. Create a test table.
        2. Take a backup and refresh burst cluster.
        3. Insert into the test table. It should run on burst and owned by
            burst.
        4. Begin a transaction. Change distkey on the test table to make it a
            shadow table. And then insert data into the test table, and commit.
        5. Now the test table should be owned by main, and the copy query
            should have write_query_on_tbl_with_shadow_table 49 status.

        Args:
            cluster: The cluster to run test.
        """

        with DbSession(cluster.get_conn_params()) as db_session:
            cursor = db_session.cursor()
            schema = db_session.session_ctx.schema
            self._prepare_data(cursor, self._TABLE_NAME_FOR_INSERT)
            self._start_and_wait_for_refresh(cluster)

            qid = self._run_query_to_own_burst(cursor,
                                               self._TABLE_NAME_FOR_INSERT)

            self._validate_query_burst_status(qid, BurstStatus.did_burst)
            self._validate_ownership(
                schema,
                self._TABLE_NAME_FOR_INSERT,
                True,  # is_owned_by_burst
                self._get_query_xid(qid))

            qid = self._insert_into_shadow_table(cursor,
                                                 self._TABLE_NAME_FOR_INSERT)

            self._validate_query_burst_status(
                qid, BurstStatus.write_query_on_tbl_with_shadow_table)
            self._validate_ownership(
                schema,
                self._TABLE_NAME_FOR_INSERT,
                False  # is_owned_by_burst
            )
