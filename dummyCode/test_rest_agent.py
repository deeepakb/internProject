# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import socket
import multiprocessing
import pytest

from raff.qp.qp_test import QPTest

log = logging.getLogger(__name__)


@pytest.mark.burst
@pytest.mark.burst_precommit
class TestRestAgent(QPTest):

    def socket_connect(self, server_address, sock):
        '''
        Helper method to connect to a cluster
        '''
        sock.connect(server_address)

    def test_rest_agent_concurrency(self):
        '''
        Test to ensure we can connect to the Rest Agent socket
        with 10 parallel clients
        '''
        server_address = '\0burst_rest_agent_socket'
        processes = []

        for i in range(10):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            p = multiprocessing.Process(target=self.socket_connect,
                                        args=[server_address, sock])
            processes.append(p)
            p.start()

        for p in processes:
            p.join()
