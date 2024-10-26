# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.common.dimensions import Dimensions
from test_burst_write_ctas_in_out_txn import (
    TestBurstWriteCTASInTxn, GUCS, SAME_SLICE_BURST_GUCS,
    DIFF_SLICE_BURST_GUCS, super_simulated_mode)

log = logging.getLogger(__name__)

__all__ = ["super_simulated_mode"]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(gucs=GUCS)
@pytest.mark.custom_burst_gucs(gucs=SAME_SLICE_BURST_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCtasInOutTxnSameSliceTempRefSS(TestBurstWriteCTASInTxn):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                ddl_cmd=['select_into', 'ctas'],
                in_txn=[True, False],
                sortkey=[''],
                diststyle=[''],
                temp_referenced=[True]))

    def test_burst_write_ctas_in_out_txn_same_slice_temp_ref_SS(
            self, db_session, cluster, vector, is_temp):
        """
        This test runs CTAS on Burst in txn when the number of
        slices are the same between main and burst
        """
        self.base_bw_ctas_in_out_txn(db_session, cluster, vector, is_temp)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(gucs=GUCS)
@pytest.mark.custom_burst_gucs(gucs=DIFF_SLICE_BURST_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCtasInOutTxnDiffSliceTempRefSS(TestBurstWriteCTASInTxn):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                ddl_cmd=['select_into', 'ctas'],
                in_txn=[True, False],
                sortkey=[''],
                diststyle=[''],
                temp_referenced=[True]))

    def test_burst_write_ctas_in_out_txn_diff_slice_temp_ref_SS(
            self, db_session, cluster, vector, is_temp):
        """
        This test runs CTAS on Burst in txn when the number of
        slices are different between main and burst
        """
        self.base_bw_ctas_in_out_txn(db_session, cluster, vector, is_temp)
