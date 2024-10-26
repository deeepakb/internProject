# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst
from raff.burst.burst_ds_localization import get_ds_localization_q_count
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.burst_helper import get_bursted_query_count

__all__ = [super_simulated_mode]

log = logging.getLogger(__name__)


@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs={**burst_unified_remote_exec_gucs_main(),
          'burst_enable_write_id_col': 'true'})
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_no_clean_db
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstDSLocalizationIdentity(BurstWriteTest):
    """
    Basic test suite to test identity column support in burst using data sharing
    localization.
    """

    @property
    def generate_sql_res_files(self):
        return True

    def test_burst_ds_localization_identity_basics(self, cluster, db_session):
        count_before = get_bursted_query_count(cluster)
        ds_localization_before = get_ds_localization_q_count(cluster)
        # Number of bursted queries in the YAML file.
        NUM_BURSTED_QUERIES = 7
        self.execute_test_file(
            'test_burst_ds_localization_identity_basics', session=db_session)
        count_after = get_bursted_query_count(cluster)
        ds_localization_after = get_ds_localization_q_count(cluster)
        assert count_after - count_before >= NUM_BURSTED_QUERIES
        assert ds_localization_after - ds_localization_before >= NUM_BURSTED_QUERIES
