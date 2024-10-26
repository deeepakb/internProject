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


class BWIdColVacuum(BWIdColExtraMethods):
    def _execute_extra_cmds_1(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        extra_cmd_1 = "vacuum {};"
        cursor.execute(extra_cmd_1.format(main_tbl))
        cursor.execute(extra_cmd_1.format(burst_tbl))

    def _execute_extra_cmds_2(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        extra_cmd_1 = "vacuum {} to 100 percent;"
        cursor.execute(extra_cmd_1.format(main_tbl))
        cursor.execute(extra_cmd_1.format(burst_tbl))

    def _execute_extra_cmds_3(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        extra_cmd_1 = "vacuum sort only {} to 90 percent;"
        cursor.execute(extra_cmd_1.format(main_tbl))
        cursor.execute(extra_cmd_1.format(burst_tbl))

    def _execute_extra_cmds_4(self, cluster, cursor, vector, main_tbl,
                              burst_tbl):
        extra_cmd_1 = "vacuum delete only {} to 100 percent;"
        cursor.execute(extra_cmd_1.format(main_tbl))
        cursor.execute(extra_cmd_1.format(burst_tbl))

    def _get_all_extra_test_methods(self):
        extra_methods = [
            self._execute_extra_cmds_1,
            self._execute_extra_cmds_2,
            self._execute_extra_cmds_3,
            self._execute_extra_cmds_4,
        ]
        return extra_methods


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_id_col': 'true'
    })
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
class TestBurstWriteIdColVacuumSS(BWIdColVacuum):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_burst_write_id_cols_vacuum_commit_out_txn(self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_burst_write_id_cols_vacuum_abort_out_txn(self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBWIdColVacuumCluster(BWIdColVacuum):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c1)'], sortkey=[''], fix_slice=[False]))

    def test_burst_write_id_cols_vacuum_commit_out_txn(self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods()
        self.base_bw_id_cols_commit_extra_out_txn(cluster, vector,
                                                  extra_test_methods)

    def test_burst_write_id_cols_vacuum_abort_out_txn(self, cluster, vector):
        extra_test_methods = self._get_all_extra_test_methods()
        self.base_bw_id_cols_abort_extra_out_txn(cluster, vector,
                                                 extra_test_methods)
