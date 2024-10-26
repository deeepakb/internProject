# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.burst.burst_test import get_burst_clusters_arns
from raff.util.utils import run_bootstrap_sql
from raff.common.cluster.cluster_session import ClusterSession

log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.skip_load_data
class TestBurstModeSupportedAndUnsupportedQuery(BurstWriteTest):

    CREATE_SQL1 = """create table if not exists burst_semisorted_distall(
                  a varchar(20), b varchar(20))
                  diststyle all sortkey(a);"""
    DISTALL_TABLE = "burst_semisorted_distall"
    INSERT_SQL1 = """insert into {} values
                   ('Fabian', 'Nagel'), ('Ippokratis', 'Pandis'),
                   ('Martin', 'Grund'), ('Naresh', 'Chainani'),
                   ('Foyzur', 'Rahman'), ('Nikos', 'Armenatzoglou'),
                   ('Bhavik', 'Bhuta');"""
    INSERT_SQL2 = """insert into {} values
                   ('Gaurav', 'Saxena');"""
    VALIDATE_SQL1 = "select * from {} order by a;"
    _DROP_TABLE_QUERY = "DROP TABLE IF EXISTS {}"

    GUCS = {
        "max_concurrency_scaling_clusters": "1",
        "always_burst_eligible_query": "true",
        "always_burst_eligible_query_timeout_min": "10",
        "selective_dispatch_level": "0",
        "enable_burst_failure_handling": "false"
    }

    def test_burst_mode_no_burst_cluster(self, cluster):
        """
        Test to verify burst eligible query with no prior burst
        cluster attached gets bursted
        """
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=self.GUCS), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:

            cursor.execute(self.CREATE_SQL1)
            cursor.execute(self.INSERT_SQL1.format(self.DISTALL_TABLE))
            cluster.release_all_burst_clusters()
            # Read query as burst eligible query
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(self._DROP_TABLE_QUERY.format(self.DISTALL_TABLE))

    def test_burst_mode_stale_burst_cluster(self, cluster):
        """
        Test to verify burst eligible query with a stale burst cluster
        attached gets bursted
        """
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=self.GUCS), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:

            cursor.execute(self.CREATE_SQL1)
            cursor.execute(self.INSERT_SQL1.format(self.DISTALL_TABLE))
            # Tests whether burst-ineligible query (write query on distall)
            # runs on main
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            # Write query on distall table as burst-ineligible query
            # to make burst cluster stale
            cursor.execute(self.INSERT_SQL2.format(self.DISTALL_TABLE))
            self._check_last_query_didnt_burst(cluster, cursor)
            cluster_arns = get_burst_clusters_arns(cluster)
            assert cluster_arns is not None
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(self._DROP_TABLE_QUERY.format(self.DISTALL_TABLE))


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.skip_load_data
class TestBurstModeFailBurstQuery(BurstWriteTest):

    CREATE_SQL1 = """create table if not exists burst_semisorted_distall(
                  a varchar(20), b varchar(20))
                  diststyle all sortkey(a);"""
    DISTALL_TABLE = "burst_semisorted_distall"
    INSERT_SQL1 = """insert into {} values
                   ('Fabian', 'Nagel'), ('Ippokratis', 'Pandis'),
                   ('Martin', 'Grund'), ('Naresh', 'Chainani'),
                   ('Foyzur', 'Rahman'), ('Nikos', 'Armenatzoglou'),
                   ('Bhavik', 'Bhuta');"""
    INSERT_SQL2 = """insert into {} values
                   ('Gaurav', 'Saxena');"""
    VALIDATE_SQL1 = "select * from {} order by a;"
    _DROP_TABLE_QUERY = "DROP TABLE IF EXISTS {}"

    GUCS = {
        "max_concurrency_scaling_clusters": "1",
        "always_burst_eligible_query": "true",
        "always_burst_eligible_query_timeout_min": "10",
        "selective_dispatch_level": "0",
        "enable_burst_failure_handling": "false"
    }

    def _setup(self, cursor, cluster):
        cursor.execute(self.CREATE_SQL1)
        cursor.execute(self.INSERT_SQL1.format(self.DISTALL_TABLE))

    def test_burst_mode_fail_personalize(self, cluster):
        """
        Test to verify burst eligible query fails if
        burst personalization keeps failing
        """
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=self.GUCS), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:
            with cluster.event("EtSimulateBurstManagerError", "error=Personalize"):

                self._setup(cursor, cluster)
                # Release burst cluster if any
                cluster.run_xpx('burst_release_all')
                # Burst eligible query fails due to failed personalization
                cursor.execute_failing_query(
                    self.VALIDATE_SQL1.format(self.DISTALL_TABLE),
                    "Could not burst query")
                cursor.execute(self._DROP_TABLE_QUERY.format(self.DISTALL_TABLE))

    def test_burst_mode_fail_burst(self, cluster):
        """
        Test to verify if burst eligible query fails when executing on burst,
        it doen't run on main and fails with
        error message
        """
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=self.GUCS), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:

            self._setup(cursor, cluster)
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            cluster_arn = get_burst_clusters_arns(cluster)
            assert len(cluster_arn) == 1
            # Set event to simulate query executing on burst fail
            cluster.run_xpx(
                'burst_set_event {} EtSimulateRestAgentQueryError'
                .format(cluster_arn[0]))
            # Add additional insert query to inavlidate the result
            # cache for the select query
            cursor.execute(self.INSERT_SQL2.format(self.DISTALL_TABLE))
            self._check_last_query_didnt_burst(cluster, cursor)
            # Burst eligible query fails
            cursor.execute_failing_query(
                self.VALIDATE_SQL1.format(self.DISTALL_TABLE),
                "Could not execute query")
            cluster.run_xpx(
                'burst_unset_event {} EtSimulateRestAgentQueryError'
                .format(cluster_arn[0]))
            cursor.execute(self._DROP_TABLE_QUERY.format(self.DISTALL_TABLE))

    def test_burst_mode_fail_on_refresh(self, cluster):
        """
        Test to verify burst eligible query fails if
        burst commit based refresh keeps failing
        """
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=self.GUCS), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:

            self._setup(cursor, cluster)
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            cluster_arn = get_burst_clusters_arns(cluster)
            assert len(cluster_arn) == 1
            # Set event to disable commit based refresh on burst cluster
            cluster.run_xpx(
                'burst_set_event {} EtDisableBurstCommitRefreshOnBurst'
                .format(cluster_arn[0]))
            # Query runs on main making burst stale
            cursor.execute(self.INSERT_SQL2.format(self.DISTALL_TABLE))
            self._check_last_query_didnt_burst(cluster, cursor)
            # Burst eligible query fails due to stale burst cluster
            cursor.execute_failing_query(
                self.VALIDATE_SQL1.format(self.DISTALL_TABLE),
                "Could not burst query")
            cluster.run_xpx(
                'burst_unset_event {} EtDisableBurstCommitRefreshOnBurst'
                .format(cluster_arn[0]))
            cursor.execute(self._DROP_TABLE_QUERY.format(self.DISTALL_TABLE))

    def test_burst_mode_failure_handling(self, cluster):
        """
        Test to verify that burst failure handling works
        by running burst eligible query on main after
        failing to run on burst
        This is done by setting enable_burst_failure_handling
        to true
        """
        gucs = {
            "max_concurrency_scaling_clusters": "1",
            "always_burst_eligible_query": "true",
            "always_burst_eligible_query_timeout_min": "10",
            "selective_dispatch_level": "0",
            "enable_burst_failure_handling": "true"
        }
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=gucs), \
             DbSession(cluster.get_conn_params()).cursor() as cursor:
            self._setup(cursor, cluster)
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_bursted(cluster, cursor)
            cluster_arn = get_burst_clusters_arns(cluster)
            assert len(cluster_arn) == 1
            # Set event to simulate query executing on burst fail
            cluster.run_xpx(
                'burst_set_event {} EtSimulateRestAgentQueryError'
                .format(cluster_arn[0]))
            # Burst eligible query fails on burst and runs on main
            cursor.execute(self.VALIDATE_SQL1.format(self.DISTALL_TABLE))
            self._check_last_query_didnt_burst(cluster, cursor)
