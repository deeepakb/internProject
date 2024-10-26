# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from test_burst_write_id_col_extra_methods import BWIdColExtraMethods
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


class BWIdColAlterDistSortEncode(BWIdColExtraMethods):
    """
    This test file contains test cases for running alter diststyle even,
    alter distkey, alter sortkey and alter column encode committed/aborted
    within/without transaction block.
    The tests would utilize the general test methods.
    """

    def _execute_alter_cmds_1(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter diststyle even;"
        alter_cmd_2 = ("alter table {} alter column c0 encode zstd, "
                       "alter column c1 encode zstd;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _execute_alter_cmds_2(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter distkey c0, alter sortkey (c0);"
        alter_cmd_2 = ("alter table {} alter column c0 encode bytedict, "
                       "alter column c1 encode bytedict;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _execute_alter_cmds_3(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter distkey c1, alter sortkey (c1);"
        alter_cmd_2 = ("alter table {} alter column c0 encode lzo, "
                       "alter column c1 encode lzo;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _execute_alter_cmds_4(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter distkey c0, alter sortkey (c0,c1);"
        alter_cmd_2 = ("alter table {} alter column c0 encode az64, "
                       "alter column c1 encode az64;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _execute_alter_cmds_5(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter sortkey none;"
        alter_cmd_2 = ("alter table {} alter column c2 encode delta, "
                       "alter column c1 encode delta;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _execute_alter_cmds_6(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        alter_cmd_1 = "alter table {} alter sortkey (c1,c2);"
        alter_cmd_2 = ("alter table {} alter column c2 encode runlength, "
                       "alter column c1 encode runlength;")
        cursor.execute(alter_cmd_1.format(main_tbl))
        cursor.execute(alter_cmd_1.format(burst_tbl))
        cursor.execute(alter_cmd_2.format(main_tbl))
        cursor.execute(alter_cmd_2.format(burst_tbl))

    def _get_all_extra_test_methods(self, sortkey_none):
        extra_methods = [
            self._execute_alter_cmds_1,
            self._execute_alter_cmds_2,
            self._execute_alter_cmds_3,
            self._execute_alter_cmds_4,
            self._execute_alter_cmds_6,
        ]
        # Alter sortkey none cannot run inside transaction block
        # So it is only included in tests without transaction block
        if sortkey_none:
            extra_methods.append(self._execute_alter_cmds_5)
        return extra_methods


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '4',
        'burst_enable_write': 'true',
        'burst_enable_write_id_col': 'true'
    })
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
class TestBWIdColAlterDistSortEncodeSS(BWIdColAlterDistSortEncode):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_bw_id_cols_alter_distsort_encode_commit_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(True)
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_abort_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(True)
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_commit_in_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(False)
        self.base_bw_id_cols_commit_extra_in_txn(cluster, vector,
                                                 extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_abort_in_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(False)
        self.base_bw_id_cols_abort_extra_in_txn(cluster, vector,
                                                extra_test_methods)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBWIdColAlterDistSortEncodeCluster(BWIdColAlterDistSortEncode):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_bw_id_cols_alter_distsort_encode_commit_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(True)
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_abort_out_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(True)
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_commit_in_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(False)
        self.base_bw_id_cols_commit_extra_in_txn(cluster, vector,
                                                 extra_test_methods)

    def test_bw_id_cols_alter_distsort_encode_abort_in_txn(
            self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods(False)
        self.base_bw_id_cols_abort_extra_in_txn(cluster, vector,
                                                extra_test_methods)
