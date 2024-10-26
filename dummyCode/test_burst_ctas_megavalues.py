# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_mv_gucs
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_ctas import TestBurstWriteCTASBase
from raff.storage.megablock_suite import MegablockSuite

log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]


def megavalue_gucs():
    '''
    Helper function to get megavalue gucs from MegablockSuite.
    '''
    megavalue_obj = MegablockSuite()
    return megavalue_obj.get_megavalue_gucs()


CUSTOM_BURST_GUCS = dict(list(burst_write_mv_gucs.items()) +
                         list(megavalue_gucs().items()) + [('slices_per_node', '3')])


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(gucs=dict(list(megavalue_gucs().items()) +
                                         [('burst_enable_write_user_ctas',
                                           'false')]))
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_BURST_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstReadMegavaluesCTASSS(TestBurstWriteCTASBase):
    def test_burst_ctas_megavalues_ss(self, cluster):
        self.base_test_burst_ctas(cluster, has_megavalues=True)
