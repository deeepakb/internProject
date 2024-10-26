# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import os


from contextlib import contextmanager
from raff.burst.burst_test import BurstTest
from raff.common.simulated_helper import (
    XEN_ROOT, SimDbConnectError)
from io import open


log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestBurstColdStartGeneral(BurstTest):

    @contextmanager
    def _touch_file(self, file_path):
        """
        Creates a new file. Cleans up this file at the end of execution.

        :type file_path: string
        :param file_path: a file path relative to XEN_ROOT
        """
        file_path = '{}/{}'.format(XEN_ROOT, file_path)
        with open(file_path, 'w+'):
            pass  # frees file handle immediately rather than wait for gc
        try:
            yield
        finally:
            os.remove(file_path)

    def test_burst_cluster_restart(self, cluster, cluster_session):
        """
        This test validates that the cluster restart does not succeed after the
        cold start.
        """
        with cluster_session(
                gucs={
                    "is_burst_cluster": "true",
                    "s3commit_tossing_mode": 0,
                    "enable_mirror_to_s3": 0,
                    "enable_commits_to_dynamo": 0
                },
                clean_db_before=True,
                clean_db_after=True):
            # create cold start status file
            with self._touch_file("data/backup/cold_start_status.txt"):
                try:
                    cluster.reboot_cluster()
                    assert False, 'Expected cluster restart to fail'
                except SimDbConnectError:
                    log.info("Cluster restart failed as expected.")
            # Rebooting cluster to ensure it comes up now.
            cluster.reboot_cluster()
