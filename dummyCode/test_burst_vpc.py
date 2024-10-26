# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   BASIC_CLUSTER_GUCS)
from raff.common.cluster.cluster_session import ClusterSession
from raff.util.utils import run_bootstrap_sql

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]
BACKUP_PREFIX = "burst-bk-{}"

CHECK_BURST_STATUS_SQL = """
select concurrency_scaling_status from svl_query_concurrency_scaling_status
where query = {}
"""

NON_EXISTING_VPCE = {'burst_mode': '3', 'burst_proxy_endpoint': 'ciao.mamma'}

@pytest.mark.cluster_only
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestBurstNonExistingVPCE(BurstTest):
    def test_burst_non_existing_vpce(self, db_session, cluster,
                                     cluster_session):
        '''
        Burst a query and verify it didn't burst because the cluster VPCE is
        not setup properly. Since the VPCE is not setup this test CANNOT send
        any network message to the burst cluster, so the version of the burst
        cluster is not relevant.
        '''
        gucs = dict(BASIC_CLUSTER_GUCS, **NON_EXISTING_VPCE)
        gucs["s3_stl_bucket"] = "redshift-monitoring-qa"
        # Take snapshot so burst cluster has latest database
        backup_id = BACKUP_PREFIX.format(str(uuid.uuid4())[:8])

        with cluster_session(gucs=gucs), \
            db_session.cursor() as cursor:
            cluster.backup_cluster(backup_id)
            self.execute_test_file('burst_query', session=db_session)
            qid = self.last_query_id(cursor)
            nested_lst = run_bootstrap_sql(cluster,
                                           CHECK_BURST_STATUS_SQL.format(qid))
            assert len(nested_lst) == 1, (
                "No rows returned for query {}".format(qid))
            assert len(nested_lst[0]) == 1, (
                "No columns returned for query {}".format(qid))
            status = nested_lst[0][0]
            assert status == '24', (
                "Status verification failed for qid {}".format(qid))


NON_VPC_CLUSTER = {
    'burst_mode': '3',
    # Fake instance metadata of a non-vpc cluster
    'instance_metadata_url': 'file://$XEN_ROOT/test/fake_instance_metadata'
}


# Note: this is a burst mode 3 test that runs in localhost only because
# we need a fake instance metadata to pretend we don't have a VPC, and that
# file doesn't exists on a cluster. We can't use the custom burst gucs
# fixture because otherwise the test will be fixed.
@pytest.mark.localhost_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestBurstNonVPCCluster(BurstTest):
    def test_burst_non_vpc_cluster(self, db_session, cluster):
        '''
        Burst a query and verify it didn't burst because the cluster is not
        a VPC cluster.
        '''
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=NON_VPC_CLUSTER):
            with self.db.cursor() as bootcursor, db_session.cursor() as cursor:
                self.execute_test_file('burst_query', session=db_session)
                qid = self.last_query_id(cursor)
                bootcursor.execute(CHECK_BURST_STATUS_SQL.format(qid))
                status = bootcursor.fetch_scalar()
                assert status == 23, (
                    "Status verification failed for qid {}".format(qid))
