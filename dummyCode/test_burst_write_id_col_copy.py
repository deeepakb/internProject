# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from test_burst_write_id_col_extra_methods import BWIdColExtraMethods
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_temp_write import burst_user_temp_support_gucs

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]
COPY_ERROR_LOG = ("select starttime,trim(filename) as filename,"
                  "colname,trim(raw_line) as raw_line,err_code,"
                  "err_reason from stl_load_errors order by 1;")


class BWIdColCopy(BWIdColExtraMethods):
    def get_burst_cluster_copy_error_log(self):
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute(COPY_ERROR_LOG)
            log.info("COPY burst cluster error: {}".format(
                str(burst_cursor.fetchall())))

    def _execute_one_col_copy(self, cursor, main_tbl, burst_tbl, file_idx):
        s3_path = 's3://burst-write-id-col-test/bw_id_col_1_col_{}.csv.gz'
        try:
            cursor.run_copy(
                main_tbl + "(c0)", s3_path.format(file_idx), gzip=True)
            cursor.run_copy(
                burst_tbl + "(c0)", s3_path.format(file_idx), gzip=True)
        except Exception as e:
            cursor.execute(COPY_ERROR_LOG)
            log.info("COPY exception {} error: {}".format(
                str(e), str(cursor.fetchall())))
            self.get_burst_cluster_copy_error_log()
            assert False

    def _execute_two_col_copy(self, cursor, main_tbl, burst_tbl, file_idx):
        s3_path = 's3://burst-write-id-col-test/bw_id_col_2_col_{}.csv.gz'
        try:
            cursor.run_copy(
                main_tbl + "(c0,c2)",
                s3_path.format(file_idx),
                gzip=True,
                delimiter=",")
            cursor.run_copy(
                burst_tbl + "(c0,c2)",
                s3_path.format(file_idx),
                gzip=True,
                delimiter=",")
        except Exception as e:
            cursor.execute(COPY_ERROR_LOG)
            log.info("COPY exception {} error: {}".format(
                str(e), str(cursor.fetchall())))
            self.get_burst_cluster_copy_error_log()
            assert False

    def _get_all_extra_test_methods_one_col_copy(self):
        extra_methods = []
        for i in range(10):
            copy_method = lambda cluster, cursor, vector, main_tbl,\
                burst_tbl: self._execute_one_col_copy(
                    cursor, main_tbl, burst_tbl, i)
            extra_methods.append(copy_method)
        return extra_methods

    def _get_all_extra_test_methods_two_col_copy(self):
        extra_methods = []
        for i in range(10):
            copy_method = lambda cluster, cursor, vector, main_tbl,\
                burst_tbl: self._execute_two_col_copy(
                    cursor, main_tbl, burst_tbl, i)
            extra_methods.append(copy_method)
        return extra_methods


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
            gucs=dict(
                list(burst_user_temp_support_gucs.items()) + [(
                    'burst_enable_write_id_col', 'true'), ('slices_per_node', '4'),
                    ('burst_enable_write', 'true')]))
@pytest.mark.custom_local_gucs(
        gucs=dict(
            list(burst_user_temp_support_gucs.items()) + [(
                'burst_enable_write_id_col', 'true')]))
class TestBurstWriteIdColCopySS(BWIdColCopy):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_burst_write_id_cols_copy_1_cols_commit_out_txn(
            self, cluster, vector, is_temp):
        extra_test_methods = self._get_all_extra_test_methods_one_col_copy()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector, extra_test_methods,
                                                  is_temp)

    def test_burst_write_id_cols_copy_1_cols_abort_out_txn(
            self, cluster, vector, is_temp):
        extra_test_methods = self._get_all_extra_test_methods_one_col_copy()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector, extra_test_methods,
                                                 is_temp)

    def test_burst_write_id_cols_copy_2_cols_commit_out_txn(
            self, cluster, vector, is_temp):
        extra_test_methods = self._get_all_extra_test_methods_two_col_copy()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector, extra_test_methods,
                                                  is_temp)

    def test_burst_write_id_cols_copy_2_cols_abort_out_txn(
            self, cluster, vector, is_temp):
        extra_test_methods = self._get_all_extra_test_methods_two_col_copy()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector, extra_test_methods,
                                                 is_temp)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBWIdColCopyCluster(BWIdColCopy):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even'],
                sortkey=['compound sortkey(c0, c1)'],
                fix_slice=[False]))

    @pytest.mark.skip(reason="DP-63233")
    def test_burst_write_id_cols_copy_1_cols_commit_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods_one_col_copy()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_burst_write_id_cols_copy_1_cols_abort_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods_one_col_copy()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)

    def test_burst_write_id_cols_copy_2_cols_commit_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods_two_col_copy()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_burst_write_id_cols_copy_2_cols_abort_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods_two_col_copy()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)
