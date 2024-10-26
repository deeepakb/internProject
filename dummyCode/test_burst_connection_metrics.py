# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import pytest
from threading import Thread
import time
import uuid

from raff.burst.burst_test import (
    async_prepare_burst,
    BASIC_CLUSTER_GUCS,
    BURST_MODE_3,
    BurstTest,
    setup_sample_data)
from raff.common.region import Regions
from raff.common.ssh_user import SSHUser
from raff.monitoring.monitoring_test import MonitoringTestSuite
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstConnectionMetrics(BurstTest, MonitoringTestSuite):
    """
    Test burst connection CloudWatch metrics are emitted.
    """
    metric_file_path = "/dev/shm/redshift/monitoring/burst/"
    metric_file = "/dev/shm/redshift/monitoring/burst/stats"
    attempt_metric_name = "ConcurrencyScalingConnectionAttempts"
    failure_metric_name = "ConcurrencyScalingConnectionFailures"
    success_metric_name = "ConcurrencyScalingConnectionSuccesses"
    network_proxy_metric = "NetworkProxy"
    auth_proxy_metric = "AuthProxy"
    network_error_metric = "NetworkConnection"
    burst_connection_network_proxy_error_type = 0
    burst_connection_auth_proxy_error_type = 1
    burst_connection_network_error_type = 2
    # Burst cluster gucs to test with, override BurstMode to cluster mode.
    burst_cluster_gucs = dict(BASIC_CLUSTER_GUCS, **{"burst_mode": BURST_MODE_3})
    # DP RAFF testing is done in QA regions.
    region = Regions.QA

    def watch_cw_metric_file(self, ssh, metric_file, metric_name_list, result,
                             retries=150, delay_seconds=2):
        """
        Helper function used to execute a unix/linux command on the remote host, and
        assert that the contents of stdout is not -1.
        """
        for _ in range(0, retries):
            try:
                cmd = '''cat {} '''.format(metric_file)
                _, raw_ouput, _ = ssh.run_remote_cmd(cmd, user=SSHUser.RDSDB, retries=5)
                output = raw_ouput.strip()
                log.info("Metric list:{} output:{}".format(metric_name_list, output))
                for i, metric in enumerate(metric_name_list):
                    if metric in output:
                        # Metric was found during this test period, mark result.
                        result[i] = True

                for i, found in enumerate(result):
                    assert found, "Did not find Metric:{} Output:{}".format(
                        metric_name_list[i], output)
            except AssertionError:
                time.sleep(delay_seconds)
                continue
            return
        # After max retries, assert failure
        assert False, "Did not find Metrics: {} after Retries: {} Result:{}".format(
            metric_name_list, retries, result)

    def wait_for_metric_file_update(self, cluster, metric_name_list, result):
        """
        Helper function to wait for the metric file to be created on the remote host.
        """
        result[0] = False
        ssh = cluster.get_leader_ssh_conn()
        self.watch_cw_metric_file(ssh, self.metric_file, metric_name_list, result)

    @pytest.mark.cluster_only
    def test_burst_connection_network_proxy_error(self, request, cluster,
                                                  cluster_session):
        # Create thread to verify cw metrics in the background.
        test_metric_list = [self.network_proxy_metric, self.attempt_metric_name]
        result = [False] * len(test_metric_list)
        thread = Thread(target=self.wait_for_metric_file_update, args=(
            cluster, test_metric_list, result))

        # Setup test cluster with configuration, data, and backup.
        with cluster_session(gucs=self.burst_cluster_gucs):
            setup_sample_data(cluster)
            acquired_burst_cluster = False
            SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                                                  str(uuid.uuid4().hex)))
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)

            # Using EtBurstConnectionTesting event to throw error for network proxy.
            with cluster.event('EtBurstConnectionTesting', 'type={}'.format(
                    self.burst_connection_network_proxy_error_type)):
                try:
                    # Run background thread to check metrics, then attempt acquire.
                    thread.start()
                    async_prepare_burst(
                        cluster, request.config.option.burst_build_to_acquire,
                        timeout_s=300)
                    acquired_burst_cluster = True
                    log.info("Did not catch async prepare burst exception")
                except Exception as e:
                    log.info("Caught async prepare burst exception: {}".format(e))

                # No burst cluster should have been acquired
                assert acquired_burst_cluster is False
                # Expected metrics should have been found.
                thread.join()
                for i, found in enumerate(result):
                    assert found, "Did not find Metric:{} ".format(test_metric_list[i])

    @pytest.mark.cluster_only
    def test_burst_connection_auth_proxy_error(self, request, cluster,
                                               cluster_session):
        # Create thread to verify cw metrics in the background.
        test_metric_list = [self.auth_proxy_metric, self.attempt_metric_name]
        result = [False] * len(test_metric_list)
        thread = Thread(target=self.wait_for_metric_file_update, args=(
            cluster, test_metric_list, result))

        # Setup test cluster with configuration, data, and backup.
        with cluster_session(gucs=self.burst_cluster_gucs):
            setup_sample_data(cluster)
            acquired_burst_cluster = False
            SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                                                  str(uuid.uuid4().hex)))
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)

            # Using EtBurstConnectionTesting event to throw error for network proxy.
            with cluster.event('EtBurstConnectionTesting', 'type={}'.format(
                    self.burst_connection_auth_proxy_error_type)):
                try:
                    # Run background thread to check metrics, then attempt acquire.
                    thread.start()
                    async_prepare_burst(
                        cluster, request.config.option.burst_build_to_acquire,
                        timeout_s=300)
                    acquired_burst_cluster = True
                    log.info("Did not catch async prepare burst exception")
                except Exception as e:
                    log.info("Caught async prepare burst exception: {}".format(e))

                # No burst cluster should have been acquired
                assert acquired_burst_cluster is False
                # Expected metrics should have been found.
                thread.join()
                for i, found in enumerate(result):
                    assert found, "Did not find Metric:{} ".format(test_metric_list[i])

    @pytest.mark.cluster_only
    def test_burst_connection_network_error(self, request, cluster, cluster_session):
        # Create thread to verify cw metrics in the background.
        test_metric_list = [self.network_error_metric, self.attempt_metric_name]
        result = [False] * len(test_metric_list)
        thread = Thread(target=self.wait_for_metric_file_update, args=(
            cluster, test_metric_list, result))

        # Setup test cluster with configuration, data, and backup.
        with cluster_session(gucs=self.burst_cluster_gucs):
            setup_sample_data(cluster)
            acquired_burst_cluster = False
            SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                                                  str(uuid.uuid4().hex)))
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)

            # Using EtBurstConnectionTesting event to throw error for network.
            with cluster.event('EtBurstConnectionTesting', 'type={}'.format(
                    self.burst_connection_network_error_type)):
                try:
                    # Run background thread to check metrics, then attempt acquire.
                    thread.start()
                    async_prepare_burst(
                        cluster, request.config.option.burst_build_to_acquire,
                        timeout_s=300)
                    acquired_burst_cluster = True
                    log.info("Did not catch async prepare burst exception")
                except Exception as e:
                    log.info("Caught async prepare burst exception: {}".format(e))

                # No burst cluster should have been acquired
                assert acquired_burst_cluster is False
                # Expected metrics should have been found.
                thread.join()
                for i, found in enumerate(result):
                    assert found, "Did not find Metric:{} ".format(test_metric_list[i])

    @pytest.mark.cluster_only
    def test_burst_connection_success(self, request, cluster, cluster_session):
        # Create thread to verify cw metrics in the background.
        test_metric_list = [self.success_metric_name]
        result = [False] * len(test_metric_list)
        thread = Thread(target=self.wait_for_metric_file_update, args=(
            cluster, test_metric_list, result))

        # Setup test cluster with configuration, data, and backup.
        with cluster_session(gucs=self.burst_cluster_gucs):
            setup_sample_data(cluster)
            SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                                                  str(uuid.uuid4().hex)))
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)

            try:
                # Run background thread to check metrics, then attempt acquire.
                thread.start()
                async_prepare_burst(
                    cluster, request.config.option.burst_build_to_acquire)
            except Exception as e:
                log.info("Caught async prepare burst exception: {}".format(e))
                assert False, ("Unable to acquire burst cluster successfully: "
                               "{}".format(e))

            # Expected metrics should have been found.
            thread.join()
            for i, found in enumerate(result):
                assert found, "Did not find Metric:{} ".format(test_metric_list[i])
