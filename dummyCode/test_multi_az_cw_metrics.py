# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import logging

import os
from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.common.node_type import NodeType
from raff.burst.burst_test import BurstTest
from contextlib import contextmanager
from raff.common.remote_helper import RedMachine
from raff.common.ssh_user import SSHUser
from raff.common.region import Regions
from raff.common import retry
from io import open
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
class TestBurstCWMazMetrics(BurstTest, MonitoringTestSuite):
    """
    Test burst MAZ metrics for CW are emitted.
    """

    query_failures = \
        "MultiAzSecondaryQueryFailures"
    not_attached = \
        "MultiAzClustersWithoutSecondaryAttached"
    personalization_failures = \
        "MultiAzFailedPersonalization"

    METRIC_NOT_FOUND_MSG = """
        Unable to find metric files: {}. Reenable assert here after:
        https://issues.amazon.com/issues/ArcadiaDP-323"""
    metric_file_path = "/dev/shm/redshift/monitoring/multi_az/"
    metric_file = "/dev/shm/redshift/monitoring/multi_az/internal"
    # Events for triggering behavior
    personalization_failure_event = "EtSimulateRestAgentPersonalizationError"
    query_failure_event = "EtSimulateRestAgentQueryError"
    # DP testing is normally performed on QA region
    # TODO(mzibits) update to use click option etc.
    region = Regions.QA

    def setup(self):
        self.cleanup_monitoring_directory()

    def teardown(self):
        pass

    @contextmanager
    def run_burst_event(self, cluster, event):
        try:
            secondary = self.get_maz_secondary_cluster(cluster)
            cluster.run_xpx("burst_set_event {} {}".format(secondary, event))
            yield
        finally:
            cluster.run_xpx("burst_unset_event {} {}".format(secondary, event))

    @retry(retries=150, delay_seconds=1)
    def watch_cw_metric_file(self, ssh, cmd, metric_name):
        """
        Helper function used to execute a unix/linux command on the remote host, and
        assert that the contents of stdout is not -1.
        """
        r, o, e = ssh.run_remote_cmd(cmd, user=SSHUser.RDSDB)
        check_metric_exists = o.find(metric_name)
        assert check_metric_exists != -1, "{} not found on remote host.".format(
            metric_name)

    def check_cw_metric_file(self, ssh, cmd):
        """
        Helper function used to execute a unix/linux command on the remote host,
        then return stdout to the caller.
        """
        r, o, e = ssh.run_remote_cmd(cmd, user=SSHUser.RDSDB)
        return o

    def wait_for_metric_file_update(self, leader_ip, region, metric_name):
        """
        Helper function to wait for the metric file to be created on the remote host.
        """
        cmd_0 = '''grep -i {} {}'''.format(metric_name, self.metric_file)
        cmd_1 = '''cat {}'''.format(self.metric_file)
        with RedMachine(host=leader_ip, region=region) as ssh:
            self.watch_cw_metric_file(ssh, cmd_0, metric_name)
            file_text = self.check_cw_metric_file(ssh, cmd_1)
            log.info("remote file contents: {}".format(file_text))
            if not os.path.exists(self.metric_file_path):
                log.info("file path {} does not exist. Creating now.".format(
                    self.metric_file_path))
                os.makedirs(self.metric_file_path)
            with open(self.metric_file, 'w+') as local_metric_file:
                local_metric_file.write(file_text)
                log.info("local file contents: {}".format(
                    local_metric_file.read()))

    def check_if_remote_file_exists(self, cluster, metric_name):
        """
        Helper function to check if the metric file exists on the remote host.
        """
        leader_ip = cluster.leader_public_ip
        log.info("MAZ primary leader_ip: {}.".format(leader_ip))
        # Test that metrics file exists
        self.wait_for_metric_file_update(leader_ip, self.region, metric_name)

    # Adapted from test_arcadia_cw_metrics.py
    def validate_count_metric_from_cluster(self,
                                           cluster,
                                           metric_name,
                                           expected_value,
                                           check_gt=False):
        """
        Helper function to wait for a metric to populate itself in the metrics
        file and validate it matches.
        """
        self.check_if_remote_file_exists(cluster, metric_name)
        # Evaluate correct value
        actual_value = self.read_value_for_metric(self.metric_file, "value",
                                                  ["name", metric_name])
        if actual_value is None:
            log.error(
                "Could not find metric %s in metrics file." % metric_name)
        if check_gt:
            assert actual_value > expected_value
        else:
            assert actual_value == expected_value

    # Adapted from test_arcadia_cw_metrics.py
    def validate_count_metric_gt(self, cluster, metric_name, expected_value):
        self.validate_count_metric_from_cluster(
            cluster, metric_name, expected_value, check_gt=True)

    def verify_metrics(self, db_session, cluster, metric):
        with db_session.cursor() as cursor:
            # Trigger query and verify
            cursor.execute("select count(*) from catalog_sales")
        self.validate_count_metric_from_cluster(cluster, metric, 1)

    def enable_maz_cw_metric_helper(self, db_session, cluster, cluster_session,
                                    event, metric):
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
            with self.run_burst_event(cluster, event):
                # Trigger a backup and a refresh of the burst cluster
                # after loading the data
                self.trigger_backup_and_refresh(cluster)
                self.verify_metrics(db_session, cluster, metric)

    @pytest.mark.skip("reason=RedshiftDP-48801")
    def test_maz_failed_query(self, db_session, cluster, cluster_session):
        """
        In this test we set the EtSimulateRestAgentQueryError event on the
        secondary cluster and validate that the metric MultiAzSecondaryQueryFailures
        was emitted in Cloud Watch.
        """
        self.enable_maz_cw_metric_helper(db_session, cluster, cluster_session,
                                         self.query_failure_event,
                                         self.query_failures)

    @pytest.mark.skip("reason=RedshiftDP-48802")
    def test_maz_failed_personalization(self, db_session, cluster,
                                        cluster_session):
        """
        In this test we set the EtSimulateRestAgentPersonalizationError event on the
        secondary cluster and validate that the metric MultiAzFailedPersonalization
        was emitted in Cloud Watch.
        """
        self.enable_maz_cw_metric_helper(db_session, cluster, cluster_session,
                                         self.personalization_failure_event,
                                         self.personalization_failures)

    def test_maz_not_attached(self, db_session, cluster, cluster_session):
        """
        In this test we set the EtSimulateRestAgentPersonalizationError event on the
        secondary cluster, and validate that the metric MultiAzClustersWithoutSecondaryAttached
        was emitted in Cloud Watch.
        """
        self.enable_maz_cw_metric_helper(db_session, cluster, cluster_session,
                                         self.personalization_failure_event,
                                         self.not_attached)
