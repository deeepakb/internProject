# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import http.server
import pytest
import requests
import ssl
import threading
import time

from raff.common.simulated_helper import XEN_ROOT
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

HM_STATUS = True
HM_PORT = 1120
HM_URL = "https://localhost:{}".format(HM_PORT)
HM_BURST_BACKUP_API_LOC = "/rds/backup/startAsync"
HM_BURST_BACKUP_PREFIX = "burst-sys-"


class BackupRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/shutdown":
            self.server.running = False
            self.send_response(200)
            self.end_headers()
            return

        if not self.server.in_progress and self.path.startswith(
                HM_BURST_BACKUP_API_LOC):
            content_len = int(self.headers["Content-Length"])
            input = self.rfile.read(content_len)
            input = input.decode('utf-8')
            self.protocol_version = 'HTTP/1.1'
            if input.startswith("backupId"):
                try:
                    backup_id = input.split('=')[1]
                    if backup_id.startswith(HM_BURST_BACKUP_PREFIX):
                        self.send_response(200)
                    else:
                        self.send_response(400)
                except Exception as e:
                    self.send_response(400, str(e))
            else:
                self.send_response(400)
            self.send_header("Content-Length", 0)
            self.end_headers()
            return

        if self.server.in_progress and self.path.startswith(
                HM_BURST_BACKUP_API_LOC):
            self.send_response(409)
            self.send_header("Content-Length", 0)
            self.end_headers()
            return

        if self.path.startswith("/flip"):
            self.server.in_progress = not self.server.in_progress
            self.send_response(200, "Backup State Changed")
            self.end_headers()
            return

        self.send_error(404, "Wrong URL")


class HMMockServer:
    def __init__(self):
        self._server = http.server.HTTPServer(('', HM_PORT),
                                                 BackupRequestHandler)
        self._server.socket = ssl.wrap_socket(
            self._server.socket,
            certfile='{}/test/raff/burst/MockHM.pem'.format(XEN_ROOT),
            server_side=True)
        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._server.in_progress = False

    def run(self):
        self._server.running = True
        while self._server.running:
            self._server.handle_request()

    def start(self):
        self._thread.start()

    def shutdown(self):
        requests.post(HM_URL + "/shutdown", verify=False)
        time.sleep(2)
        self._thread.join()
        self._server.socket.close()

    def flip(self):
        requests.post(HM_URL + "/flip", verify=False)


@pytest.yield_fixture(scope="function")
def hm_wrapper():
    server = HMMockServer()
    server.start()
    yield server
    server.shutdown()


CUSTOM_AUTO_GUCS = {"hm_ondemand_backup_uri": HM_URL + HM_BURST_BACKUP_API_LOC,
                    "hm_burst_backup_prefix": "burst-sys-"}


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstBackupLocally(BurstTest):
    def test_burst_backup_local(self, cluster, hm_wrapper):
        """Testing that we can trigger taking a backup."""
        cluster.run_xpx('burst_backup')

    def test_burst_no_backup_local(self, cluster):
        """If HM is not available, we cannot take a backup."""
        try:
            cluster.run_xpx('burst_backup')
            assert False, "HM is up and should be down."
        except Exception as e:
            assert e is not None

    def test_backup_in_progress(self, cluster, hm_wrapper):
        """This tests asserts that when a backup is already in progress, the
        call to the HM still succeeds."""
        hm_wrapper.flip()
        cluster.run_xpx('burst_backup')


@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstBackupCluster(BurstTest):
    def test_burst_backup_cluster(self, cluster):
        """Tests that the xpx command is able to successfully take a
        backup on a cluster."""
        cluster.run_xpx('burst_backup')
