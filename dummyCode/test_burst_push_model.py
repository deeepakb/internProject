# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import (super_simulated_mode)
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.storage.storage_test import disable_all_autoworkers

log = logging.getLogger(__name__)

__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

burst_push_model_gucs = {
    'enable_burst_async_acquire': 'true',
    'enable_burst_fast_create_push_model': 'true',
    'enable_burst_fast_create_push_model_service_eni': 'true',
}


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=burst_push_model_gucs)
class TestBurstPushModel(BurstTest):

    def _reset_burst_push_model_(self, cluster):
        cluster.run_xpx("burst_clean_cluster_map")
        cluster.run_xpx("burst_clean_push_notification_map")

    def _add_expected_push_notification(self, cluster, num_nodes, arn):
        output_list = cluster.run_xpx("burst_push_model_request_cluster {} {}".format(
            num_nodes, arn))
        if not output_list or len(output_list) < 1:
            raise AssertionError("burst_push_model_request_cluster xpx missing "
                                 "expected output")
        output = output_list[0]
        assert "Waiting on push-notification for cluster with ARN: '{}'".format(
            arn) in output, "burst_push_model_request_cluster xpx unexpected output." \
            " Output:{}".format(output)

    def _send_push_notification(self, cluster, burst_cluster_arn,
                                preshared_key, endpoint):
        return cluster.run_xpx("burst_fast_create_acquire {} {} {}".format(
            burst_cluster_arn, preshared_key, endpoint))

    @pytest.mark.parametrize(
        "burst_cluster_requests",
        [[(2, "arn2")],
         [(3, "arn3")],
         [(4, "arn4")],
         [(2, "arn2")],
         [(2, "arn2"), (3, "arn3"), (4, "arn4")],
         [(2, "arn2"), (3, "arn3"), (4, "arn4"), (5, "arn5")]])
    def test_burst_push_model(self, cluster, burst_cluster_requests):
        """
        Unit-test Burst Push-model cluster acquisition by using test xpx command
        burst_push_model_request_cluster and super-simulated mode. End-to-end
        integration testing is being done as an additional item.
        """
        # Example values to test push-notifications. The specific values do not
        # matter, since we skip burst cluster connections in this test.
        default_preshared_key = "ABC"
        default_endpoint = "192.168.2.1"

        # Add expected burst cluster entries for push-model, in main cluster.
        burst_cluster_arn_list = []
        for request in burst_cluster_requests:
            num_nodes = request[0]
            arn = request[1]
            burst_cluster_arn_list.append(arn)
            self._add_expected_push_notification(cluster,
                                                 num_nodes,
                                                 arn)

        # Send test push-notifications and verify success.
        for burst_cluster_arn in burst_cluster_arn_list:
            output_list = self._send_push_notification(cluster,
                                                       burst_cluster_arn,
                                                       default_preshared_key,
                                                       default_endpoint)
            if not output_list:
                raise AssertionError(
                    "burst_fast_create_acquire xpx missing output")
            output = output_list[-1]
            log.info(output)
            assert "Acquired cluster with ARN: '{}'".format(burst_cluster_arn) \
                in output, "Burst cluster arn should have been returned, but " \
                "not found"

            # Test ARN should have been used already.
            output_list = self._send_push_notification(
                cluster, burst_cluster_arn, default_preshared_key,
                default_endpoint)
            if not output_list:
                raise AssertionError(
                    "burst_fast_create_acquire xpx missing output")
            output = output_list[-1]
            log.info(output)
            assert "Could not find cluster arn in the cluster map" in output, \
                "Should not have found cluster arn in cluster map "

        # Clean burst state for test clean-up.
        self._reset_burst_push_model_(cluster)
