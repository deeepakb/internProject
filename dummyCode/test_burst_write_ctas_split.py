# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import verify_all_queries_bursted
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, verify_all_queries_bursted]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true'
})
@pytest.mark.custom_local_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true',
    'ctas_auto_analyze': 'false'
})
class TestBurstCtasWithSplitCtas(BurstWriteTest):
    def _check_all_ctas_bursted(self, cluster):
        with self.db.cursor() as cursor:
            query = ("select distinct query from stl_internal_query_details"
                     " where query_cmd_type = 4")
            cursor.execute(query)
            ctas_queries = cursor.fetchall()[0]
        for query in ctas_queries:
            self.verify_query_bursted(cluster, query)

    def test_burst_write_ctas_split(self, cluster):

        db_session = DbSession(cluster.get_conn_params(user='master'))
        self.execute_test_file("setup_test_burst_ctas_split", db_session)
        self._start_and_wait_for_refresh(cluster)
        self.execute_test_file("test_burst_ctas_split", db_session)
        self._check_all_ctas_bursted(cluster)
