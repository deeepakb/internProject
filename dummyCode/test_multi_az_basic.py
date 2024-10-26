# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import datetime
import uuid

from raff.common.node_type import NodeType
from raff.burst.burst_test import BurstTest

pytestmark = pytest.mark.node_info(
    launch_node_type=NodeType.MULTI_AZ_I3EN_XLPLUS,
    unsupported_node_types=NodeType.non_ra3_types)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.load_tpcds_data
@pytest.mark.no_burst_nightly
@pytest.mark.encrypted_only
@pytest.mark.ra3_only
class TestMultiAzBasic(BurstTest):

    def test_multi_az_basic(self, db_session, cluster, cluster_session):
        custom_gucs = {
            "try_multi_az_first": "true",
            "selective_dispatch_level": "0",
            "legacy_stl_disablement_mode": 0
        }
        with cluster_session(gucs=custom_gucs), \
             cluster.event('EtBurstFindClusterTracing', "level=ElDebug5"), \
             db_session.cursor() as cursor:

            # Wait for secondary to be reacquired after the restart to
            # change the gucs
            self.wait_for_multi_az_reacquired(cluster)

            # Trigger a backup and a refresh of the burst cluster
            # after loading the data
            self.trigger_backup_and_refresh(cluster)

            # Trigger query and verify
            cursor.execute("select count(*) from catalog_sales")
            self.check_last_query_ran_on_multi_az(cluster, cursor)
