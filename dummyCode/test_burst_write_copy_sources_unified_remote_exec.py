# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst
from raff.burst.test_burst_copy_sources_base import TestBurstCopyDynamoBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_unified_remote_exec_gucs_burst().items()) +
        [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true')]))
@pytest.mark.custom_local_gucs(gucs=burst_unified_remote_exec_gucs_main())
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyDynamoUnifiedRemoteExec(TestBurstCopyDynamoBase):
    def test_burst_copy_dynamo_unified_remote_exec(self, cluster, db_session):
        self.base_test_burst_copy_dynamo(
            cluster, db_session, is_temp=False, unified_remote_exec=True)
