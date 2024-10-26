# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from __future__ import division
import logging
import pytest
import time
import uuid
import multiprocessing

from contextlib import contextmanager

from psycopg2.extensions import QueryCanceledError
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session_context import SessionContext
from raff.burst.burst_test import (
    BurstTest,
    setup_teardown_burst
)
from raff.util.utils import run_bootstrap_sql
from raff.common.host_type import HostType
from raff.common.profile import Profiles
from raff.common.region import DEFAULT_REGION

log = logging.getLogger(__name__)


# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]


CUSTOM_GUCS = dict(
    burst_mode='3',
    enable_short_query_bias='false',
    enable_burst_async_acquire='false')
QUERY_RUNNING_TIMEOUT_MINUTES = 5
TEMP_USER = "Regular" + str(uuid.uuid4().hex)


@pytest.mark.load_tpcds_data
@pytest.mark.no_jdbc
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstQueryUserId(BurstTest):

    def test_burst_query_userid(self, cluster):
        """
        Test the user of query executed on Burst cluster is not bootstrap
        user but the one configure in GUC (burst_cluster_superuser_id).

        Args:
            cluster (RedshiftCluster): Redshift cluster instance
        """

        query_uuid = str(uuid.uuid4().hex)
        long_query = ("select count(*) /* {} */ from "
                      "catalog_sales A, catalog_sales B, "
                      "catalog_sales C").format(query_uuid)

        with self._temp_regular_user(cluster):
            db = RedshiftDb(cluster.get_conn_params())
            p = multiprocessing.Process(target=self._execute_long_query,
                                        args=(db, long_query, ))
            p.start()

            try:
                primary_qid, pid = self._wait_until_query_running(
                    cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)
                burst_cluster_id = self._get_burst_cluster_id(cluster,
                                                              primary_qid)
                assert burst_cluster_id, "Query {} didn't burst.".format(
                    primary_qid)

                client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                        region=DEFAULT_REGION)
                burst_cluster = client.describe_cluster(burst_cluster_id)
                burst_cluster_superuser_id = \
                    self._get_burst_cluster_superuser_id(burst_cluster)
                self._verify_burst_user(
                    burst_cluster, query_uuid, burst_cluster_superuser_id)
            except Exception:
                raise
            finally:
                if 'pid' in locals():
                    query = "select pg_terminate_backend({});".format(pid)
                    run_bootstrap_sql(cluster, query)

            p.join()

    @contextmanager
    def _temp_regular_user(self, cluster):
        with RedshiftDb(cluster.get_conn_params()) as db:
            with db.cursor() as cursor:
                cursor.execute("CREATE USER \"{}\" "
                               "WITH PASSWORD '{}'".format(
                                   TEMP_USER, "Testing1234"))
                cursor.execute('GRANT ALL ON DATABASE "{}" TO \"{}\"'.format(
                    "dev", TEMP_USER))
        yield
        with RedshiftDb(cluster.get_conn_params()) as db:
            with db.cursor() as cursor:
                cursor.execute(
                    'REVOKE ALL ON DATABASE "{}" FROM \"{}\"'.format(
                        "dev", TEMP_USER))
                cursor.execute('DROP USER \"{}\"'.format(TEMP_USER))

    def _execute_long_query(self, db, long_query):
        """
        Execute the test query.

        Args:
            db (RedshiftDb): Redshift database connection
            long_query (str): Query to run
        """

        try:
            with db.cursor() as cursor:
                cursor.execute(
                    "set session authorization \"{}\"".format(TEMP_USER))
                cursor.execute("set query_group to burst")
                cursor.execute(long_query)
        except QueryCanceledError:
            pass

    def _wait_until_query_running(self, cluster, query_uuid, timeout_minutes):
        """
        Wait the long query to start running.

        Args:
            cluster (RedshiftCluster): Redshift cluster instance
            query_uuid (str): Uuid used for searching the query
            timeout_minutes (int): Timeout value for waiting the query to run

        Return:
            A tuple of (query id integer, process id integer)
        """

        query = ("select query, pid from stv_inflight "
                 "where text like '%/* {} */%' and label = 'burst';"
                 "").format(query_uuid)

        delay_seconds = 10
        retries = timeout_minutes * 60 // delay_seconds

        with DbSession(cluster.get_conn_params(),
                       session_ctx=SessionContext(
                       user_type=SessionContext.MASTER,
                       host_type=HostType.CLUSTER)) as db:
            with db.cursor() as cursor:
                for _ in range(retries):
                    cursor.execute("set query_group to metrics")
                    cursor.execute(query)
                    result = cursor.fetchall()
                    if result:
                        return result[0]
                    time.sleep(delay_seconds)
        pytest.fail("Timeout for waiting the long query to run")

    def _get_burst_cluster_id(self, cluster, primary_qid):
        """
        Get Burst cluster where query is executed.

        Args:
            cluster (RedshiftCluster): Cluster instance
            primary_qid (int): Query ID

        Return:
            Cluster ID string or None if query didn't burst
        """

        query = ("set query_group to metrics; "
                 "select cluster_arn from stl_burst_query_execution where "
                 "query={} limit 1").format(primary_qid)
        rows = run_bootstrap_sql(cluster, query)
        if rows:
            arn = rows[0][0]
            return arn.split(':cluster:')[-1].strip()
        else:
            return None

    def _get_burst_cluster_superuser_id(self, burst_cluster):
        """
        Get superuser of Burst cluster.
        It is configured in GUC burst_cluster_superuser_id.

        Args:
            burst_cluster (RedshiftCluster): Cluster instance

        Return:
            Superuser Id string.
        """

        query = "show burst_cluster_superuser_id"
        burst_cluster_superuser_id = \
            run_bootstrap_sql(burst_cluster, query)[0][0]
        return burst_cluster_superuser_id

    def _verify_burst_user(self, burst_cluster, query_uuid, superuser):
        """
        Verify the user of query executed on Burst cluster is superuser.

        Args:
            burst_cluster (RedshiftCluster): Cluster instance
            query_uuid (str): Uuid used for searching the query
            superuser (str): User id of superuser on Burst cluster
        """

        query = ("set query_group to metrics; "
                 "select userid from stv_inflight "
                 "where text like '%/* {} */%' and text not like "
                 "'select userid from stv_inflight%';").format(query_uuid)

        result = run_bootstrap_sql(burst_cluster, query)
        assert result, "Didn't find query running on burst cluster"
        userid = result[0][0]
        assert userid == superuser
