# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import uuid
import datetime
from raff.data.data_utils import load_table
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (
    get_burst_cluster_arn,
    setup_teardown_burst,
    verify_query_bursted,
    verify_query_didnt_burst
)
from raff.common.cluster.cluster_helper import RedshiftClusterHelper
__all__ = [
    setup_teardown_burst, verify_query_bursted, verify_query_didnt_burst
]

log = logging.getLogger('test_burst_alter_sortkey')

ALTER_STMT = "alter table customer_alter_sortkey alter sortkey (c_nationkey)"

BURST_QUERY_VALIDATION = '''
select backup_id from stl_burst_query_execution where len(btrim(error)) = 0
and xid='{}'
'''

GET_QUERY_XID = '''
select xid from stl_querytext where query = {}
'''

# TPCH query #8
BURST_CANARY_QUERY = '''
select count(*) from customer_alter_sortkey;
'''


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstAlterSortkey(BurstTest):

    def take_snapshot(self, cluster):
        """
        Take a snapshot.

        Args:
            cluster: cluster object
        """
        snapshot_identifier = ("{}-{}".format(
            cluster.cluster_identifier,
            str(uuid.uuid4().hex)))
        log.info("take snapshot {}".format(snapshot_identifier))
        cluster.backup_cluster(snapshot_identifier)
        return snapshot_identifier

    def setup_data(self, db_session):
        """
        Create the load the data to be used in the test.

        Args:
            db_session: db_session object
        """
        load_table(db=db_session, dataset_name='tpch',
                   table_name='customer', scale='1',
                   table_name_suffix='_alter_sortkey',
                   grant_to_public=True)

    def test_burst_alter_sortkey_with_burst(
            self, db_session, cluster, verify_query_bursted):
        """
        Test runs ALTER SORTKEY on a table and confirms it can still
        have queries run against it burst successfully.
        """
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(db_session)

            # RUN ALTER COMMAND
            log.info("run alter sortkey")
            cursor.execute(ALTER_STMT)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))

            # Run query and validate the query was bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                burst_db_cursor.execute(BURST_CANARY_QUERY)

            cursor.execute("drop table if exists customer_alter_sortkey")
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)

    def test_burst_alter_sortkey_without_burst(
            self, db_session, cluster, verify_query_didnt_burst):
        """
        Test runs ALTER SORTKEY on a table and confirms it does not burst
        if no new snapshot is taken.
        """
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(db_session)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))

            # RUN ALTER COMMAND
            log.info("run alter sortkey")
            cursor.execute(ALTER_STMT)

            # Run query and validate the query was bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                burst_db_cursor.execute(BURST_CANARY_QUERY)

            cursor.execute("drop table if exists customer_alter_sortkey")
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)
