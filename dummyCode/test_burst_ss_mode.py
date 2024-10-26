# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid

import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import BurstTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.skip(reason=("Super simulated not ready"))
@pytest.mark.precommit
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.load_tpcds('call_center')
@pytest.mark.backup_and_cold_start(SNAPSHOT_IDENTIFIER)
class TestBurstSSMode(BurstTest):
    def test_basic_super_simulated_mode(self, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        self.execute_test_file('burst_query', session=db_session)
