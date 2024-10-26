# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
import logging

from test_burst_write_id_col_burst_error_in_txn_no_retry import (
    burst_write_retry_gucs, BaseBurstIDColClusterError)
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import disable_all_autoworkers
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode_method)

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode_method, disable_all_autoworkers]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '6',
        'burst_enable_write': 'true',
        'burst_enable_write_id_col': 'true',
        'vacuum_auto_worker_enable': 'false',
        'burst_blk_hdr_stream_threshold': 1
    })
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.custom_local_gucs(gucs=burst_write_retry_gucs)
class TestBurstIDColClusterErrorOutTxnRetry(BaseBurstIDColClusterError):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c0)'],
                sortkey=[''],
                validate_query_mode=['burst'],
                dml=['insert', 'insert_select', 'delete', 'update'],
                event=[
                    'EtFakeBurstErrorStepDelete', 'EtFakeBurstErrorStepInsert',
                    'EtFakeBurstErrorBeforeStreamHeader',
                    'EtFakeBurstErrorAfterStreamHeader',
                    'EtFakeBurstErrorAfterSnapIn1',
                    'EtFakeBurstErrorAfterSnapIn2'
                ]))

    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_bw_id_col_burst_cluster_error_out_txn_with_retry(
            self, cluster, db_session, vector):
        """
        Test: Fails write query on different position in burst cluster, ensure
              query failure, then run write on main/burst cluster and check
              table content for further validation.
        """
        if self._should_skip(vector):
            pytest.skip("Unsupported dimension")

        assert cluster.get_guc_value(
            'burst_enable_insert_failure_handling') == 'on'

        self.base_burst_id_col_cluster_error_out_txn(cluster, db_session,
                                                     vector, True)
