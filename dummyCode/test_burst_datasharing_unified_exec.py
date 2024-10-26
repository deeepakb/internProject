# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import pytest

from contextlib import contextmanager
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
import raff.burst.remote_exec_helpers as helpers
from raff.common.base_test import DatabaseRegistry
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]
dbnames = ["pdb_ds_burst", "cdb_ds_burst"]


# SQL for fetching the table rowcount.
NUM_TABLE_ROWS = ("select count(*) from {db}.{schema}.{table}")
# SQL for fetching the table content.
GET_TABLE_CHECKSUM_CONTENT = (
    "select "
    "sum(cast(coalesce(checksum(cr_returned_date_sk), 5) as int8)) as c1, "
    "sum(cast(coalesce(checksum(cr_returned_time_sk), 5) as int8)) as c2, "
    "sum(cast(coalesce(checksum(cr_item_sk), 5) as int8)) as c3, "
    "sum(cast(coalesce(checksum(cr_refunded_customer_sk), 5) as int8)) as c4, "
    "sum(cast(coalesce(checksum(cr_refunded_cdemo_sk), 5) as int8)) as c5, "
    "sum(cast(coalesce(checksum(cr_refunded_hdemo_sk), 5) as int8)) as c6, "
    "sum(cast(coalesce(checksum(cr_refunded_addr_sk), 5) as int8)) as c7, "
    "sum(cast(coalesce(checksum(cr_returning_customer_sk), 5) as int8)) as c8, "
    "sum(cast(coalesce(checksum(cr_returning_cdemo_sk), 5) as int8)) as c9, "
    "sum(cast(coalesce(checksum(cr_returning_hdemo_sk), 5) as int8)) as c10, "
    "sum(cast(coalesce(checksum(cr_returning_addr_sk), 5) as int8)) as c11, "
    "sum(cast(coalesce(checksum(cr_call_center_sk), 5) as int8)) as c12, "
    "sum(cast(coalesce(checksum(cr_catalog_page_sk), 5) as int8)) as c13, "
    "sum(cast(coalesce(checksum(cr_ship_mode_sk), 5) as int8)) as c14, "
    "sum(cast(coalesce(checksum(cr_warehouse_sk), 5) as int8)) as c15, "
    "sum(cast(coalesce(checksum(cr_reason_sk), 5) as int8)) as c16, "
    "sum(cast(coalesce(checksum(cr_order_number), 5) as int8)) as c17, "
    "sum(cast(coalesce(checksum(cr_return_quantity), 5) as int8)) as c18, "
    "sum(cast(coalesce(checksum(cr_return_amount), 5) as int8)) as c19, "
    "sum(cast(coalesce(checksum(cr_return_tax), 5) as int8)) as c20, "
    "sum(cast(coalesce(checksum(cr_return_amt_inc_tax), 5) as int8)) as c21, "
    "sum(cast(coalesce(checksum(cr_fee), 5) as int8)) as c22, "
    "sum(cast(coalesce(checksum(cr_return_ship_cost), 5) as int8)) as c23, "
    "sum(cast(coalesce(checksum(cr_refunded_cash), 5) as int8)) as c24, "
    "sum(cast(coalesce(checksum(cr_reversed_charge), 5) as int8)) as c25, "
    "sum(cast(coalesce(checksum(cr_store_credit), 5) as int8)) as c26, "
    "sum(cast(coalesce(checksum(cr_net_loss), 5) as int8)) as c27 "
    "from {db}.{schema}.{table}")
INSERT_INTO_TABLE_STMT = (
    "insert into {tgt_db}.{tgt_schema}.{tgt_tbl} "
    "select * from {src_db}.{src_schema}.{src_tbl}")
DELETE_FROM_TABLE_STMT = (
    "delete from {tgt_db}.{tgt_schema}.{tgt_tbl} where {column} > {val}")
UPDATE_TABLE_STMT = (
    "update {tgt_db}.{tgt_schema}.{tgt_tbl} set {column} = {newval} "
    "where {column} = {oldval}")


@pytest.yield_fixture(scope="function")
def datasharing_scenario_setup(db_session, cluster):
    try:
        devdb_session = RedshiftDb(cluster.get_conn_params(db_name="dev"))
        with devdb_session.cursor() as cursor:
            # Create producer and consumer databases.
            cursor.execute("CREATE DATABASE pdb_ds_burst ISOLATION LEVEL SNAPSHOT")
            cursor.execute("CREATE DATABASE cdb_ds_burst ISOLATION LEVEL SNAPSHOT")
            cursor.execute("CREATE USER p_user PASSWORD 'Password1'")
            cursor.execute("CREATE USER c_user PASSWORD 'Password1'")
            cursor.execute("SET search_path TO public")
            cursor.execute("GRANT CREATE ON DATABASE pdb_ds_burst to p_user")
            cursor.execute("GRANT CREATE ON DATABASE cdb_ds_burst to c_user")
            yield
    finally:
        devdb_session = RedshiftDb(cluster.get_conn_params(db_name="dev"))
        with devdb_session.cursor() as cursor:
            cursor.execute("RESET SESSION AUTHORIZATION")
            cursor.execute("DROP USER IF EXISTS p_user, c_user")
            cursor.execute("DROP DATABASE pdb_ds_burst")
            cursor.execute("DROP DATABASE cdb_ds_burst")


# enable_burst_datasharing needs to be on, because the test is to test bursting
# x-db remote tables, essentially bursting DS queries.
@pytest.mark.no_jdbc
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs=dict(helpers.burst_unified_remote_exec_gucs_burst(slices_per_node=3)))
@pytest.mark.custom_local_gucs(
    gucs={**helpers.burst_unified_remote_exec_gucs_main(),
          'enable_burst_datasharing': 'true',
          'enable_burst_write_for_datasharing': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstDatasharingUnifiedExec(BurstWriteTest):
    db_registry = DatabaseRegistry(dbnames)

    @contextmanager
    def burst_db_session(self, db_session, query_group_name):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to {}".format(query_group_name))
            yield cursor
            log.info("reset burst query_group")
            cursor.execute("reset query_group")

    def _setup_tables(self, db_session, schema, suffix):
        diststyle_key = 'key distkey cr_returned_date_sk'
        sortkey = 'cr_item_sk'
        tbl_name_key_suffix = "{}_{}".format("_key", suffix)
        self._setup_table(db_session, schema, 'catalog_returns', 'tpcds', '1',
                          diststyle_key, sortkey, tbl_name_key_suffix)

    def _validate_table_row_counts_match(self, cluster, cursor, db1, schema1,
                                         tbl1, db2, schema2, tbl2):
        # Fetch number of rows in db1.schema1.table1.
        cursor.execute(NUM_TABLE_ROWS.format(
            db=db1, schema=schema1, table=tbl1))
        db1_row_count = cursor.fetch_scalar()
        # Fetch number of rows in db2.schema2.table2.
        cursor.execute(NUM_TABLE_ROWS.format(
            db=db2, schema=schema2, table=tbl2))
        db2_row_count = cursor.fetch_scalar()
        # Validate table rowcounts match.
        assert db1_row_count == db2_row_count

    def _validate_table_contents_match(self, cluster, cursor, db1, schema1,
                                       tbl1, db2, schema2, tbl2):
        # Fetch the table contents in db1.schema1.table1.
        cursor.execute(GET_TABLE_CHECKSUM_CONTENT.format(
            db=db1, schema=schema1, table=tbl1))
        db1_content = cursor.fetchall()
        # Fetch the table contents in db2.schema2.table2.
        cursor.execute(GET_TABLE_CHECKSUM_CONTENT.format(
            db=db2, schema=schema2, table=tbl2))
        db2_content = cursor.fetchall()
        # Validate table rowcounts match.
        assert db1_content == db2_content

    def _create_tables_on_producer_and_consumer(self, cluster):
        # Set things up on producer db.
        with RedshiftDb(cluster.get_conn_params(
                            db_name="pdb_ds_burst")) as pdb_conn:
            with pdb_conn.cursor() as p_cursor:
                p_cursor.execute("SET SESSION AUTHORIZATION p_user")
                p_cursor.execute("CREATE SCHEMA p_schema")
                p_cursor.execute(
                    "GRANT USAGE ON SCHEMA p_schema to c_user")
                p_cursor.execute(
                    "GRANT CREATE ON SCHEMA p_schema to c_user")
            # Load tables in the producer database.
            self._setup_tables(pdb_conn, schema="p_schema", suffix="main")
            self._setup_tables(pdb_conn, schema="p_schema", suffix="burst")
        # Set things up on consumer db.
        with RedshiftDb(cluster.get_conn_params(
                            db_name="cdb_ds_burst")) as cdb_conn:
            with cdb_conn.cursor() as c_cursor:
                c_cursor.execute("SET SESSION AUTHORIZATION c_user")
                c_cursor.execute("CREATE SCHEMA c_schema")
            # Load tables in the consumer database.
            self._setup_tables(cdb_conn, schema="c_schema", suffix="main")
            self._setup_tables(cdb_conn, schema="c_schema", suffix="burst")
        # Refresh the burst cluster after ingestion.
        # TODO(kgudi): Remove once we drop dependency on refresh.
        self._start_and_wait_for_refresh(cluster)

    def test_burst_write_insert_from_datasharing_tables(
            self, cluster, datasharing_scenario_setup):
        """
        In this test we validate executing datasharing (cross-db) read/write
        queries on concurrency scaling clusters. In the test, we create a
        producer and consumer database, load tables, execute cross-db queries,
        validate cross-db queries run on burst cluster and finally validate the
        data integrity of the tables.
        """
        self._create_tables_on_producer_and_consumer(cluster)
        with RedshiftDb(cluster.get_conn_params(
                            db_name="cdb_ds_burst")) as cdb_conn:
            # Execute cross-db queries where source tables are from producer
            # database and target tables are from consumer database.
            with self.burst_db_session(
                    cdb_conn, query_group_name="burst") as c_cursor:
                c_cursor.execute("SET SESSION AUTHORIZATION c_user")
                # Validate table rowcounts match on producer and consumer.
                self._validate_table_row_counts_match(
                    cluster, c_cursor, db1="pdb_ds_burst", schema1="p_schema",
                    tbl1="catalog_returns_key_burst", db2="cdb_ds_burst",
                    schema2="c_schema", tbl2="catalog_returns_key_burst")
                self._validate_table_contents_match(
                    cluster, c_cursor, db1="pdb_ds_burst", schema1="p_schema",
                    tbl1="catalog_returns_key_burst", db2="cdb_ds_burst",
                    schema2="c_schema", tbl2="catalog_returns_key_burst")
                # Read from remote table and insert into local table.
                c_cursor.execute(INSERT_INTO_TABLE_STMT.format(
                    tgt_db="cdb_ds_burst", tgt_schema="c_schema",
                    tgt_tbl="catalog_returns_key_burst", src_db="pdb_ds_burst",
                    src_schema="p_schema", src_tbl="catalog_returns_key_main"))
                # Validate insert from remote table into local table did burst.
                self._check_last_query_bursted(cluster, c_cursor)
                # Validate the storage OID state on burst against local table
                # for tables localized on burst.
                helpers.validate_storage_oid_on_burst_against_main(
                    cluster, remote_host_name="main_cluster",
                    remote_db_name="cdb_ds_burst",
                    remote_schema_name="c_schema",
                    remote_table_name="catalog_returns_key_burst")
                helpers.validate_storage_oid_on_burst_against_main(
                    cluster, remote_host_name="localhost",
                    remote_db_name="pdb_ds_burst",
                    remote_schema_name="p_schema",
                    remote_table_name="catalog_returns_key_main")
            # Repeat the same operation of INSERT into SELECT *, where source
            # is local table executing on main and validate the table contents.
            with self.burst_db_session(
                    cdb_conn, query_group_name="noburst") as c_cursor:
                c_cursor.execute("SET SESSION AUTHORIZATION c_user")
                c_cursor.execute(INSERT_INTO_TABLE_STMT.format(
                    tgt_db="cdb_ds_burst", tgt_schema="c_schema",
                    tgt_tbl="catalog_returns_key_main", src_db="cdb_ds_burst",
                    src_schema="c_schema", src_tbl="catalog_returns_key_main"))
                self._check_last_query_didnt_burst(cluster, c_cursor)
                self._validate_table_row_counts_match(
                    cluster, c_cursor, db1="cdb_ds_burst", schema1="c_schema",
                    tbl1="catalog_returns_key_burst", db2="cdb_ds_burst",
                    schema2="c_schema", tbl2="catalog_returns_key_main")
