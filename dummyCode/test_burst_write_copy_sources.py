# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.burst.test_burst_copy_sources_base import TestBurstCopyDynamoBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_copy': 'true',
    'burst_use_ds_localization': 'false'
})
@pytest.mark.custom_local_gucs(gucs={
    **burst_user_temp_support_gucs,
    'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyDynamo(TestBurstCopyDynamoBase):
    def test_burst_copy_dynamo(self, cluster, db_session, is_temp):
        self.base_test_burst_copy_dynamo(
            cluster, db_session, is_temp, unified_remote_exec=False)
