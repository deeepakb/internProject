# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import threading
import uuid
import datetime
import time

import pytest
from raff.burst.burst_test import BurstTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.db.session_context import SessionContext

log = logging.getLogger(__name__)

BASE_MULTI_AZ_GUCS = {
    'burst_mode': '1',
    'multi_az_enabled': 'true',
    'is_multi_az_primary': 'true',
    'selective_dispatch_level': '0',
    'enable_result_cache': 'false',
    'enable_sqa_by_default': 'false',
    'enable_arcadia_system_views_in_provisioned_mode': 'true'
}

QUERY_STV_WLM_SC_CONFIG = '''
                        select count(*) from
                        stv_wlm_service_class_config
                        where concurrency_scaling ilike '%multi_az_only%'
                        limit 1
                        '''


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.no_jdbc
@pytest.mark.encrypted_only
class TestMultiAzRouting(BurstTest):
    def run_long_query(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute(
                "set query_group to burst; "
                "select count(*) from catalog_sales A, catalog_sales B,"
                "catalog_sales C;")

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(wlm_config=[
                '[{"concurrency_scaling":"auto", "query_concurrency": 5}]',
                '[{"concurrency_scaling":"off", "query_concurrency": 5}]',
                '[{"concurrency_scaling":"auto", "query_concurrency": 50}]',
                '[{"concurrency_scaling":"off", "query_concurrency": 50}]',
                '[{"concurrency_scaling":"auto", "auto_wlm": true}]',
                '[{"concurrency_scaling":"off", "auto_wlm": true}]',
            ]))

    def test_try_multi_az_first(self, cluster, cluster_session, db_session,
                                vector):
        '''
        Test to verify that queries are routed to multiAz when
        try_multi_az_first is set, no matter what WLM config we are using
        '''
        starttime = datetime.datetime.now().replace(microsecond=0)
        test_gucs = {
            'try_multi_az_first': 'true',
            'wlm_json_configuration': vector.wlm_config
        }
        gucs = dict(BASE_MULTI_AZ_GUCS, **test_gucs)
        with cluster_session(gucs=gucs):
            with self.db.cursor() as cursor:
                cursor.execute("select count(*) from stl_wlm_error "
                               "where recordtime > '{}'".format(starttime))
                count = cursor.fetch_scalar()
                assert count == 0

            with db_session.cursor() as cursor:
                cursor.execute("create table t1 (col1 int)")
                cursor.execute("insert into t1 values(1)")

            SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_first',
                                                  str(uuid.uuid4().hex)))

            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
            cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")

            for i in range(5):
                with db_session.cursor() as cursor:
                    cursor.execute("select now();")
                    start_time = cursor.fetch_scalar()
                    cursor.execute("select * from t1")
                    cursor.fetchall()
                    self.check_last_query_ran_on_multi_az(cluster, cursor)
                    # check if compute type of sys_query_history is secondary
                    query_text = ("SELECT count(*) "
                                  "FROM sys_query_history "
                                  "WHERE query_text = 'select * from t1;' "
                                  "AND compute_type = 'secondary' "
                                  "AND start_time > '{}' ".format(start_time))
                    cursor.execute(query_text)
                    query_result = cursor.fetch_scalar()
                    assert query_result == 1, \
                        'There should be only one record with ' \
                        'compute_type == secondary'

    def test_try_multi_az_first_wlm_service_class_config_bootstrap(
            self, cluster, cluster_session, db_session, vector):
        '''
        Test to verify that when queries are routed to multiAz
        that bootstrap user will observe concurrency_scaling as
        multi_az_only in stv_wlm_service_class_config.
        '''
        starttime = datetime.datetime.now().replace(microsecond=0)
        test_gucs = {
            'try_multi_az_first': 'true',
            'wlm_json_configuration': vector.wlm_config
        }
        gucs = dict(BASE_MULTI_AZ_GUCS, **test_gucs)
        with cluster_session(gucs=gucs):
            with self.db.cursor() as cursor:
                cursor.execute("select count(*) from stl_wlm_error "
                               "where recordtime > '{}'".format(starttime))
                count = cursor.fetch_scalar()
                assert count == 0

            conn_params = cluster.get_conn_params()
            ctx = SessionContext(user_type='bootstrap')
            with DbSession(conn_params, session_ctx=ctx).cursor() as bs:
                bs.execute("create table t1 (col1 int)")
                bs.execute("insert into t1 values(1)")

                SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_first',
                                                      str(uuid.uuid4().hex)))

                cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
                cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")
                bs.execute("select * from t1")
                bs.fetchall()
                bs.execute(QUERY_STV_WLM_SC_CONFIG)
                query_result_stv = bs.fetch_scalar()
                if 'off' in vector.wlm_config:
                    assert query_result_stv == 1, \
                        'There should be only one record with ' \
                        'concurrency_scaling == multi_az_only'
                else:
                    assert query_result_stv == 0, \
                        'There should be no latest record with ' \
                        'concurrency_scaling == multi_az_only'

    @pytest.mark.skip(reason="SysTable-753")
    def test_try_multi_az_first_wlm_service_class_config_super(
            self, cluster, cluster_session, db_session, vector):
        '''
        Test to verify that when queries are routed to multiAz
        that super user will not observe concurrency_scaling as
        multi_az_only in stv_wlm_service_class_config.
        '''
        starttime = datetime.datetime.now().replace(microsecond=0)
        test_gucs = {
            'try_multi_az_first': 'true',
            'wlm_json_configuration': vector.wlm_config
        }
        gucs = dict(BASE_MULTI_AZ_GUCS, **test_gucs)
        with cluster_session(gucs=gucs):
            with self.db.cursor() as cursor:
                cursor.execute("select count(*) from stl_wlm_error "
                               "where recordtime > '{}'".format(starttime))
                count = cursor.fetch_scalar()
                assert count == 0

            conn_params = cluster.get_conn_params()
            ctx = SessionContext(user_type='super')
            with DbSession(conn_params, session_ctx=ctx).cursor() as super:
                super.execute("create table t1 (col1 int)")
                super.execute("insert into t1 values(1)")

                SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_first',
                                                      str(uuid.uuid4().hex)))

                cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
                cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")
                super.execute("select * from t1")
                super.fetchall()
                super.execute(QUERY_STV_WLM_SC_CONFIG)
                query_result_stv = super.fetch_scalar()
                # We never display this to super user.
                assert query_result_stv == 0, \
                    'There should be no latest record with ' \
                    'concurrency_scaling == multi_az_only'

    @pytest.mark.load_tpcds_data
    def test_multi_az_routing(self, cluster, cluster_session, db_session,
                              vector):
        '''
        Test to verify that queries are routed to multiAz when concurrent
        queries are running, no matter what WLM config we are using
        '''
        starttime = datetime.datetime.now().replace(microsecond=0)
        test_gucs = {
            'try_multi_az_first': 'false',
            'wlm_json_configuration': vector.wlm_config
        }
        gucs = dict(BASE_MULTI_AZ_GUCS, **test_gucs)
        with cluster_session(gucs=gucs), \
            cluster.event('EtSimulateInflightQueryOnMazSecondary'):
            with db_session.cursor() as cursor:
                cursor.execute("create table t1 (col1 int)")
                cursor.execute("insert into t1 values(1)")

            SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_routing',
                                                  str(uuid.uuid4().hex)))

            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
            cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")

            # Start 10 long running queries
            for i in range(10):
                thread = threading.Thread(
                    target=self.run_long_query,
                    args=((DbSession(cluster.get_conn_params())), ))
                thread.start()

            # Wait for 60 seconds or until one query runs on secondary
            multi_az_count = 0
            for i in range(60):
                with self.db.cursor() as cursor:
                    cursor.execute("set query_group to metrics")
                    cursor.execute(
                        "select count(*) from stv_inflight where userid > 1")
                    total_count = cursor.fetch_scalar()
                    # Queries routed to MultiAZ Secondary have cs_status = 0
                    # in stv_inflight. However, we will leverage an event
                    # to simulate stv_inflight to show a query running
                    # on MultiAZ Secondary as cs_status = 71
                    cursor.execute("select count(*) from stv_inflight "
                                   "where concurrency_scaling_status = 71")
                    multi_az_count = cursor.fetch_scalar()
                    log.info("{}/{} queries are running "
                             "on secondary".format(multi_az_count,
                                                   total_count))
                    if total_count == 1:
                        # First query should run on main
                        assert multi_az_count == 0
                    if multi_az_count >= 1:
                        break
                time.sleep(1)
            # Verify decisions taken
            with self.db.cursor() as cursor:
                cursor.execute("set query_group to metrics")
                cursor.execute("select count(*) from stl_wlm_multi_az_routing "
                               "where routing_decision = 1")
                routed_count = cursor.fetch_scalar()
                # Note: after a while routed_count should be equal to
                # multi_az_count but we still assert to be greater
                # because the loop above breaks on the first query
                # routed to secondary, so the count might be flaky.
                log.info("routed_count: {} multi_az_count: {}".format(
                    routed_count, multi_az_count))
                assert routed_count >= multi_az_count

                # Fetch the decision for the first query
                cursor.execute(
                    "select routing_decision from stl_wlm_multi_az_routing "
                    "where recordtime >= '{}' order by recordtime asc "
                    "limit 1".format(starttime))
                routing_decision = cursor.fetch_scalar()
                # First query should always ran on main
                assert routing_decision == 0
