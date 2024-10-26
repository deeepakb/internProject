import logging

import pytest
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)

__all__ = ["setup_teardown_burst"]


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.load_tpcds_data
class TestBurstEchoBase(BurstTest):
    def test_ping_request(self, cluster, db_session):
        self.execute_test_file('burst_query', session=db_session)
        arn = self.get_localmode_burst_arn(cluster)
        results = cluster.run_xpx('burst_ping {}'.format(arn))
        line = results[0]  # type: [str]
        assert line.startswith('INFO'), "INFO not found: {}".format(line)
        result = line[line.find("<") + 1:line.find(">")]
        assert result.startswith('Ping Result'), "Result not found: {}".format(
            line)

    def test_echo_request(self, cluster, db_session):
        strict_mode = True
        outgoing_size = 16 * 1024 * 1024
        expected_size = 16 * 1024 * 1024
        self.execute_test_file('burst_query', session=db_session)
        arn = self.get_localmode_burst_arn(cluster)
        echo_cmd = 'burst_echo %s %s %d %d' % (arn, 'true'
                                               if strict_mode else 'false',
                                               outgoing_size, expected_size)
        results = cluster.run_xpx(echo_cmd)
        line = results[0]  # type: [str]
        assert line.startswith('INFO'), "INFO not found: {}".format(line)
        result = line[line.find("<") + 1:line.find(">")]
        assert result.startswith('Echo Result'), "Result not found: {}".format(
            line)
