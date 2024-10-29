import pytest
import os
import os.path
import logging
import json

from raff.monitoring.monitoring_test import MonitoringTestSuite
from io import open

log = logging.getLogger(__name__)

class TestCloudwatchOverridesConf(MonitoringTestSuite):
    """ Check whether cloudwatch_overrides.conf has the correct format.
        This will be thrown away after this feature is on boarded to CP Config Service
    """

    cw_overrides_file_locn = "/src/monitoring/api/cloudwatch_overrides.conf"

    @pytest.mark.localhost_only
    def test_cloudwatch_overrides_format(self):
        """ Check to see that cw_overrides_file exists and contains:
            1. Version field
            2. Metrics field :
                a. groups -> should be a list of lists
                b. visibility = should be either "internal" or "customer"
                c. publish = should be a boolean value
        """
        try:
            path_cw_overrides_file = os.environ["XEN_ROOT"]
            path_cw_overrides_file = path_cw_overrides_file + self.cw_overrides_file_locn
            assert os.path.isfile(path_cw_overrides_file), ("{} file not found".
                                              format(path_cw_overrides_file))
            with open(path_cw_overrides_file) as f:
                jdata = json.load(f)
                assert 'version' in jdata
                assert 'metrics' in jdata
                for metrics, desc in jdata['metrics'].items():
                    assert desc['visibility'] == 'customer' or desc['visibility'] == 'internal'
                    assert isinstance(desc['publish'], bool) == True
                    assert isinstance(desc['groups'], list) == True
                    for dimGroups in desc['groups']:
                        assert isinstance(dimGroups, list) == True
        except KeyError:
            pytest.fail("Environment variable XEN_ROOT not set")
