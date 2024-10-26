# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import datetime
import logging
import uuid
import threading
from time import sleep

from raff.common.node_type import NodeType
from raff.burst.burst_test import BurstTest
from raff.common.base_test import run_priviledged_query
from raff.common.db.session import DbSession

pytestmark = pytest.mark.node_info(
    launch_node_type=NodeType.MULTI_AZ_I3EN_XLPLUS,
    unsupported_node_types=NodeType.non_ra3_types)

log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.load_tpcds_data
@pytest.mark.no_burst_nightly
@pytest.mark.encrypted_only
@pytest.mark.no_jdbc
@pytest.mark.ra3_only
class TestMultiAzBurstAcquisition(BurstTest):
    def get_db_session(self, cluster):
        db_session = DbSession(cluster.get_conn_params())
        # Note `cursor` is not used but is creating the user
        # when the connection is opened.
        with db_session.cursor() as cursor, self.db.cursor() as cursor_db:
            # Get regular user name.
            user = db_session.session_ctx.username
            # Get the user id of the regular user.
            cursor_db.execute("select usesysid from pg_user "
                              "where usename = '{}'".format(user))
            userid = cursor_db.fetch_scalar()
            log.info("Regular user id: {}".format(userid))
            cursor_db.execute("GRANT ALL ON ALL TABLES IN SCHEMA public "
                              "to {}".format(user))
        return db_session

    def kill_background_queries(self):
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute("select pid from stv_inflight where userid > 100")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))

    def run_query(self, db_session, querytxt):
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                log.info("Executing query in background with pid " +
                         str(cursor.fetch_scalar()))
                cursor.execute(querytxt)
        except Exception as e:
            log.info("Expected to be killed: " + str(e))

    def run_background_query(self, cluster, querytxt):
        thread = threading.Thread(
            target=self.run_query,
            args=(self.get_db_session(cluster), querytxt))
        thread.start()

    def test_multi_az_burst_acquisition(self, db_session, cluster,
                                        cluster_session):
        """
        Test that when burst is disabled no burst cluster can be acquired
        even if the main cluster and the secondary cluster are busy.
        """
        cluster.wait_till_no_workflows()
        secondary = self.get_maz_secondary_cluster(cluster)
        custom_gucs = {
            "wlm_json_configuration": '[{"auto_wlm":true}]',
            "try_multi_az_first": "true",
            'enable_result_cache': 'false',
            "selective_dispatch_level": "0",
            'enable_burst_async_acquire': "false",
            'legacy_stl_disablement_mode': 0
        }
        validation_timeout = 120
        with cluster_session(gucs=custom_gucs), \
             cluster.event('EtBurstFindClusterTracing', "level=ElDebug5"), \
             cluster.event('EtOccupyAutoWlmOnMain'), \
             cluster.event('EtBurstTracing', "level=ElDebug5"), \
             cluster.event('EtFillBurstClusterSessions', "arn={}".format(secondary)), \
             self.db.cursor() as mcursor:

            # Wait for secondary to be reacquired after the restart to
            # change the gucs
            self.wait_for_multi_az_reacquired(cluster)

            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            query_uuid = str(uuid.uuid4()).replace("-", "")

            log.info(
                "Starting background thread with uuid {}".format(query_uuid))
            querytxt = """/*{}*/ select count(*)
                        from catalog_sales
                        """.format(query_uuid)
            try:
                self.run_background_query(cluster, querytxt)

                log.info("Checking prepare entries")
                success = False
                for _ in range(0, validation_timeout):
                    sleep(1)
                    query = """
                     select starttime, pid, query, btrim(error), source, multi_az_only
                     from stl_burst_prepare
                     where starttime >= '{}'
                     order by starttime desc
                     """.format(start_str)
                    with self.db.cursor() as bcursor:
                        rows = run_priviledged_query(cluster, bcursor, query)
                        for row in rows:
                            log.info("Prepare error: {}".format(row))
                            error = row[3]
                            success = True
                            if 'MultiAZ cluster' not in error:
                                # If the query gets routed to burst this assert
                                # will catch the problem.
                                assert False, "Unexpected error: {}".format(
                                    error)
                        if len(rows) > 5:
                            # If we have more than 5 entries in prepare
                            # (1 from Prepare called in workloadmanager for MAZ +
                            #  1 from Prepare called in workloadmanager for burst +
                            #  1 from Prepare called in wlmquery for MAZ +
                            #  1 from Prepare called in wlmquery for burst +
                            #  1 from Prepare called in wlmquery for a retry )
                            # it means the query is queuing and it would
                            # have been already routed to burst if possible
                            # generating an additional entry in stl_burst_prepare.
                            break
                assert success, "No entries in stl_burst_prepare"

                # Unset event, now the query is expected to run on main
                cluster.unset_event("EtOccupyAutoWlmOnMain")
                log.info("Checking execution")
                success = False
                for _ in range(0, validation_timeout):
                    sleep(1)
                    query = """ select aborted, concurrency_scaling_status, btrim(querytxt)
                           from stl_query
                           where querytxt ilike '%{}%'
                           and querytxt not ilike '%stl_query%'
                           and userid > 1
                        """.format(query_uuid)
                    # We need to use the master cursor or we don't have
                    # visibility in stl_query
                    mcursor.execute(query)
                    res = mcursor.fetchall()
                    if (len(res) == 0):
                        continue
                    log.info("stl_query entries: {}".format(res))
                    assert res[0][0] == 0, "Query should not be aborted"
                    assert res[0][1] == 0, "Query should run on main"
                    success = True
                    break
                assert success, "Query didn't run on main"

            finally:
                self.kill_background_queries()
