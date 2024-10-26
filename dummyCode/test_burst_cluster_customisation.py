# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst, customise_burst_cluster

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [customise_burst_cluster, setup_teardown_burst]


# This GUC is selected randomly as an example.
GUCS = {'burst_enable_write': 'false'}


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.no_jdbc
@pytest.mark.skip(reason="Tests were a POC and an example for usage. They run"
                         " for too long to include in normal testing.")
class BurstClusterCustomisationBase(BurstTest):
    """
    Base class to propagate serial_only, skip, cluster_only
    and no_jdbc markers.
    """
    pass


@pytest.mark.usefixtures("customise_burst_cluster")
@pytest.mark.customise_burst_cluster_args(GUCS)
class TestBurstClusterCustomisationNoInitDb(BurstClusterCustomisationBase):

    def test_burst_customisation(self, customise_burst_cluster):
        """
        This test manipulates gucs in an acquired burst cluster and verified
        that the guc changes took effect.
        """
        burst_cluster = customise_burst_cluster
        val = burst_cluster.get_padb_conf_value(key='burst_enable_write')
        assert val == 'false'


@pytest.mark.usefixtures("customise_burst_cluster")
@pytest.mark.custom_burst_gucs(gucs=GUCS, initdb_before=True)
@pytest.mark.customise_burst_cluster_args(GUCS, initdb=True)
class TestBurstClusterCustomisationInitDb(BurstClusterCustomisationBase):

    @pytest.mark.no_jdbc
    def test_burst_customisation_initdb(self, customise_burst_cluster):
        """
        This test manipulates both main and burst:
            - It issues an initdb to main and changes gucs.
            - It issues an initdb to burst and changes gucs.
        """
        burst_cluster = customise_burst_cluster
        val = burst_cluster.get_padb_conf_value(key='burst_enable_write')
        assert val == 'false'


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=GUCS, initdb_before=True)
class TestMainClusterInitDb(BurstClusterCustomisationBase):

    def test_main_cluster_initdb(self):
        """
        This test issues an initdb on main after changing gucs.
        """
        assert True
