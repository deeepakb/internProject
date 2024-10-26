# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import logging

import datetime
from contextlib import contextmanager
from raff.util.utils import run_bootstrap_sql

from raff.common.node_type import NodeType
from raff.burst.burst_test import BurstTest

pytestmark = pytest.mark.node_info(
    launch_node_type=NodeType.MULTI_AZ_I3EN_XLPLUS,
    unsupported_node_types=NodeType.non_ra3_types)

log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.load_tpcds_data
@pytest.mark.no_burst_nightly
@pytest.mark.encrypted_only
@pytest.mark.ra3_only
class TestMultiAzRebootRefresh(BurstTest):
    @contextmanager
    def block_maz_refresh(self, cluster):
        try:
            secondary = self.get_maz_secondary_cluster(cluster)
            cluster.run_xpx(
                "burst_set_event {} EtDelayBurstRefreshCompletion".format(
                    secondary))
            log.info("set XPX EtDelayBurstRefreshCompletion")
            yield
        finally:
            cluster.run_xpx(
                "burst_unset_event {} EtDelayBurstRefreshCompletion".format(
                    secondary))

    def run_query_on_maz_primary_cluster(self, cluster):
        '''
        Creates a table t1, and perfoms an INSERT query on the maz primary cluster.
        This will force a superblock refresh on the MAZ secondary cluster.

        Args:
            cluster: cluster object
        '''
        query_text_0 = "CREATE TABLE IF NOT EXISTS t1 (a int);"
        query_text_1 = """
            SET try_multi_az_first = false;
            SET SESSION_AUTHORIZATION TO master;
            SET query_group to NOBURST;
            INSERT INTO t1 VALUES (1);
            RESET query_group;
            RESET SESSION_AUTHORIZATION;
            SET try_multi_az_first = true;
        """
        run_bootstrap_sql(cluster, query_text_0)
        run_bootstrap_sql(cluster, query_text_1)

    def test_multi_az_reboot_refresh(self, db_session, cluster,
                                     cluster_session):
        """
        In this test we set an event on the secondary cluster to prevent
        refresh from completing. While refresh is in progress on the secondary
        we restart the primary cluster. When the primary starts up again
        it will reacquire the same secondary and refresh should be able
        to complete successfully. See also CP-22068.
        """
        custom_gucs = {
            "try_multi_az_first": "true",
            "selective_dispatch_level": "0",
            "legacy_stl_disablement_mode": 0
        }
        with cluster_session(gucs=custom_gucs), \
             cluster.event('EtBurstFindClusterTracing', "level=ElDebug5"):

            # Wait for secondary to be reacquired after the restart to
            # change the gucs
            self.wait_for_multi_az_reacquired(cluster)

            with self.block_maz_refresh(cluster):
                # Trigger a backup and a refresh of the burst cluster
                # with the event set, such that the refresh will not complete.
                start_time = datetime.datetime.now().replace(microsecond=0)
                start_str = start_time.isoformat(' ')
                log.info("proceeding with performing an INSERT on main...")
                self.run_query_on_maz_primary_cluster(cluster)
                log.info("started waiting for refresh to start")
                # Wait for refresh to start
                self.wait_for_refresh_to_start(
                    cluster, start_str, personalize=False, only_start=True)
                log.info("completed waiting for refresh to start")

                # Reboot the main cluster, such that the secondary cluster will
                # be reacquired with a refresh in progress
                log.info("started rebooting the cluster")
                cluster.reboot_cluster()
                log.info("completed rebooting the cluster")
                log.info(
                    "started waiting for the MAZ cluster to be reacquired")
                self.wait_for_multi_az_reacquired(cluster)
            log.info("started waiting for refresh to start")
            # Wait for refresh to finish
            self.wait_for_refresh_to_start(
                cluster, start_str, personalize=False)
            log.info("completed waiting for refresh to start")
