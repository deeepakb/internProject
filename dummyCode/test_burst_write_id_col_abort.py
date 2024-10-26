# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from test_burst_write_id_col_commit import BurstWriteIdentityColumn
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


@pytest.mark.skip(reason="rsqa-9144")
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
class TestBurstWriteIdentityColumnSSmode(BurstWriteIdentityColumn):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c1)', 'diststyle even'],
                sortkey=['', 'compound sortkey(c0, c1)'],
                fix_slice=[False, True]))

    def test_burst_write_on_id_col_abort(self, cluster, vector):
        self.base_burst_write_on_id_col_abort(cluster, vector)

    def test_burst_write_on_id_col_interleave_abort(self, cluster, vector):
        self.base_burst_write_on_id_col_interleave_abort(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteIdentityColumnCluster(BurstWriteIdentityColumn):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c1)', 'diststyle even'],
                sortkey=['', 'compound sortkey(c0, c1)'],
                fix_slice=[False]))

    def test_burst_write_on_id_col_abort(self, cluster, vector):
        self.base_burst_write_on_id_col_abort(cluster, vector)

    def test_burst_write_on_id_col_interleave_abort(self, cluster, vector):
        self.base_burst_write_on_id_col_interleave_abort(cluster, vector)
