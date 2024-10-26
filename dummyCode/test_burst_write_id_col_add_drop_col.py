# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from test_burst_write_id_col_extra_methods import BWIdColExtraMethods
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


class BWIdColAddDropCol(BWIdColExtraMethods):
    def _execute_add_col(self, cursor, main_tbl, burst_tbl, col):
        add_col_cmd = 'alter table {} add column {} bigint default 111;'
        main_cmd = add_col_cmd.format(main_tbl, col)
        burst_cmd = add_col_cmd.format(burst_tbl, col)
        cursor.execute(main_cmd)
        cursor.execute(burst_cmd)

    def _execute_drop_col(self, cursor, main_tbl, burst_tbl, col):
        drop_col_cmd = 'alter table {} drop column {};'
        main_cmd = drop_col_cmd.format(main_tbl, col)
        burst_cmd = drop_col_cmd.format(burst_tbl, col)
        cursor.execute(main_cmd)
        cursor.execute(burst_cmd)

    def _validate_added_col_data(self, cursor, main_tbl, burst_tbl, col):
        """
        New normal column data should only contain default values.
        """
        check_sql_raw = "select distinct {} from {} order by 1;"
        main_cmd = check_sql_raw.format(col, main_tbl)
        burst_cmd = check_sql_raw.format(col, burst_tbl)
        cursor.execute(main_cmd)
        res = cursor.fetchall()
        assert res == [(111, )]
        cursor.execute(burst_cmd)
        res = cursor.fetchall()
        assert res == [(111, )]

    def base_bw_id_cols_commit_extra_out_txn(self, cluster, vector):
        """
        Test burst write id col with alter add/drop column.
        Alter add column cannot add identity column. And alter drop column can
        drop identity column. Test steps:
        1. Create tables with identity columns and initializes with data on
           burst cluster.
        2. Add a column and drop a identity column.
        3. Run additional burst DMLs and validates data.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector)
            self._start_and_wait_for_refresh(cluster)
            # test generate id values from LN
            self._insert_identity_data(cluster, cursor, 3, main_tbl, burst_tbl)
            log.info("Begin validation 1")
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

            self._start_and_wait_for_refresh(cluster)
            self._insert_identity_data(cluster, cursor, 3, burst_tbl, main_tbl)
            log.info("Begin validation 2")
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

            # test generate id values from CN
            self._insert_select(cluster, cursor, 1, main_tbl, burst_tbl)
            self._start_and_wait_for_refresh(cluster)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._insert_select(cluster, cursor, 1, burst_tbl, main_tbl)

            # alter drop an id col and alter add a normal column and check
            # if burst cluster have correct column_desc
            self._execute_drop_col(cursor, main_tbl, burst_tbl, 'c2')
            self._execute_add_col(cursor, main_tbl, burst_tbl, 'c2')

            self._start_and_wait_for_refresh(cluster)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._insert_select(cluster, cursor, 3, main_tbl, burst_tbl)
            self._start_and_wait_for_refresh(cluster)
            self._insert_select(cluster, cursor, 3, burst_tbl, main_tbl)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._validate_added_col_data(cursor, main_tbl, burst_tbl, 'c2')

    def base_bw_id_cols_commit_extra_in_txn(self, cluster, vector):
        """
        Test burst write id col with alter add/drop column.
        Alter add column cannot add identity column. And alter drop column can
        drop identity column. Test steps:
        1. Create tables with identity columns and initializes with data on
           burst cluster.
        2. Drop an identity column and add a normal column.
        3. Run additional burst DMLs and validates data.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector)
            self._start_and_wait_for_refresh(cluster)
            # test generate id values from LN
            self._insert_identity_data(cluster, cursor, 3, main_tbl, burst_tbl)
            log.info("Begin validation 1")
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

            self._start_and_wait_for_refresh(cluster)
            self._insert_identity_data(cluster, cursor, 3, burst_tbl, main_tbl)
            log.info("Begin validation 2")
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

            # test generate id values from CN
            self._insert_select(cluster, cursor, 1, main_tbl, burst_tbl)
            self._start_and_wait_for_refresh(cluster)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            cursor.execute("begin;")
            self._insert_select(cluster, cursor, 1, burst_tbl, main_tbl)

            # alter drop an id col and alter add a normal column and check
            # if burst cluster have correct column_desc
            self._execute_drop_col(cursor, main_tbl, burst_tbl, 'c2')
            self._execute_add_col(cursor, main_tbl, burst_tbl, 'c2')
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._insert_select(cluster, cursor, 3, main_tbl, burst_tbl)
            self._start_and_wait_for_refresh(cluster)
            self._insert_select(cluster, cursor, 3, burst_tbl, main_tbl)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._validate_added_col_data(cursor, main_tbl, burst_tbl, 'c2')
            cursor.execute("commit;")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '6',
        'burst_enable_write': 'true',
        'burst_enable_write_id_col': 'true'
    })
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
class TestBurstWriteIdColAddDropColSS(BWIdColAddDropCol):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_bw_id_cols_add_drop_col_commit_out_txn(self, cluster, vector):
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector)

    def test_bw_id_cols_add_drop_col_commit_in_txn(self, cluster, vector):
        self.base_bw_id_cols_commit_extra_in_txn(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBWIdColAddDropColCluster(BWIdColAddDropCol):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even'], sortkey=['sortkey(c0)'],
                 fix_slice=[False]))

    def test_bw_id_cols_add_drop_col_commit_out_txn(self, cluster, vector):
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector)

    def test_bw_id_cols_add_drop_col_commit_in_txn(self, cluster, vector):
        self.base_bw_id_cols_commit_extra_in_txn(cluster, vector)
