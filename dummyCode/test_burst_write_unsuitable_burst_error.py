# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode_method
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode_method]


class BaseBurstWriteUnsuitableBurstError(BurstWriteTest):
    def unsuitable_burst_error_without_release(self, cluster):
        """
        1. set EtForceUnsuitableBurst and EtSkipBurstActiveSessionRemoval
        2. burst a query
        3. check stv_burst_manager_cluster_info and verify that
           len(active_pids) > 0
        4. select count(*) from stl_burst_connection where pid = {} is = 0
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            # Set query group to noburst preventing insert query bursted.
            # Since if the query can be bursted, a burst cluster is acquired
            # here which creates 2 burst connection log and break num_burst_connection
            # at the end line of the test.
            cursor.execute("set query_group to noburst;")
            cursor.execute("create table burst4900(c0 int) diststyle even;")
            cursor.execute("insert into burst4900 values(0);")
        log.info("cluster type {}".format(cluster.__class__.__name__))
        self._start_and_wait_for_refresh(cluster)
        with cluster.event('EtForceUnsuitableBurst'), \
            cluster.event('EtSkipBurstActiveSessionRemoval'), \
            db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            cursor.execute("set query_group to burst;")
            cursor.execute("insert into burst4900 values(0);")
            bs_cursor.execute(
                "select btrim(active_pids) from stv_burst_manager_cluster_info;"
            )
            active_pids = bs_cursor.fetchall()
            log.info("active_pids: {}".format(active_pids))
            active_pids_list = list(active_pids[0][0].strip('][').split(', '))
            log.info("active_pids_list: {}".format(active_pids_list))
            assert len(active_pids_list) > 0
            bs_cursor.execute(
                "select count(*) from stl_burst_connection where pid = {};".
                format(active_pids_list[0]))
            num_burst_connection = bs_cursor.fetchall()
            log.info("num_burst_connection: {}".format(num_burst_connection))
            # In burst_manager, we check for unsuitable burst cluster after
            # personalization. This means that there will be 2 personalization
            # connections logged in stl_burst_connection (open/close).
            # Therefore, we assert that num_burst_connections == 2.
            assert num_burst_connection == [(2, )]

    def unsuitable_burst_error_with_release(self, cluster):
        """
        1. set EtForceUnsuitableBurst and unset EtSkipBurstActiveSessionRemoval
        2. burst a query
        3. check stv_burst_manager_cluster_info and verify that
           len(active_pids) = 0
        4. select count(*) from stl_burst_connection where pid = {} is = 0
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            cursor.execute("create table burst4900(c0 int) diststyle even;")
            cursor.execute("insert into burst4900 values(0);")
        log.info("cluster type {}".format(cluster.__class__.__name__))
        self._start_and_wait_for_refresh(cluster)
        with cluster.event('EtForceUnsuitableBurst'),\
            db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            cursor.execute("set query_group to burst;")
            cursor.execute("insert into burst4900 values(0);")
            bs_cursor.execute(
                "select btrim(active_pids) from stv_burst_manager_cluster_info;"
            )
            active_pids = bs_cursor.fetchall()
            log.info("active_pids: {}".format(active_pids))
            active_pids_list = list(active_pids[0][0].strip('][').split(', '))
            log.info("active_pids_list: {}".format(active_pids_list))
            assert len(active_pids_list[0]) == 0
            bs_cursor.execute(
                "select count(*) from stl_burst_connection where "
                "pid = pg_backend_pid();")
            num_burst_connection = bs_cursor.fetchall()
            log.info("num_burst_connection: {}".format(num_burst_connection))
            assert num_burst_connection == [(0, )]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
class TestBurstWriteUnsuitableBurstError(BaseBurstWriteUnsuitableBurstError):
    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_unsuitable_burst_error_without_release(self, cluster):
        self.unsuitable_burst_error_without_release(cluster)

    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_unsuitable_burst_error_with_release(self, cluster):
        self.unsuitable_burst_error_with_release(cluster)
