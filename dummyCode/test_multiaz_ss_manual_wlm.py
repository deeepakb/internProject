# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
import getpass
import threading
import uuid
import time

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode, \
                                                         get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
SINGLE_QUEUE_WLM = '[{"query_group": ["burst"], "concurrency_scaling": "auto", "query_concurrency": 2, "user_group": ["burst"]}]'


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_tpcds("catalog_sales")
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.encrypted_only
# We need to disable try_burst_first because
# we want to route based on memory usage instead
@pytest.mark.custom_local_gucs(
    gucs={
        'selective_dispatch_level': '0',
        'multi_az_enabled': 'true',
        'try_burst_first': 'false',
        'is_multi_az_primary': 'true',
        'wlm_json_configuration': SINGLE_QUEUE_WLM
    })
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestMultiAzManualWlmRouting(BurstWriteTest):
    def run_long_query(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute(
                "select count(*) from catalog_sales A, catalog_sales B,"
                "catalog_sales C;")

    def test_multiaz_manual_wlm_routing(self, cluster, db_session):
        """
        This test verifies that total memory and used memory of
        secondary clusters is set properly when handling multiAZ.
        """
        main_wlm = cluster.get_guc_value('wlm_json_configuration')
        log.info("Main WLM: {}".format(main_wlm))
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute('show wlm_json_configuration')
            log.info("Burst WLM: {}".format(burst_cursor.fetchall()))

        # Burst a query to populate burst cluster info
        with self.db.cursor() as q_cur:
            q_cur.execute("set try_burst_first to true")
            q_cur.execute("set session_authorization to master")
            q_cur.execute("set query_group to burst")
            q_cur.execute("select * from catalog_sales")
            self.check_last_query_bursted(cluster, q_cur)
            q_cur.execute("reset session_authorization")

        # Modify the burst cluster to be a multiAZ cluster instead
        with self.db.cursor() as boot_cur:
            boot_cur.execute("select * from stv_burst_manager_cluster_info "
                             "limit 1")
            rows = boot_cur.fetchall()
            if (len(rows) == 0):
                assert False, "No entries in stv_burst_manager_cluster_info"
            log.info("Acquired cluster: {}".format(rows))
            arn = rows[0][0]
            cluster.run_xpx("update_burst_cluster {} MultiAZ true".format(arn))

        # Start 10 long running queries
        for i in range(10):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()

        # Wait for 60 seconds or until one query runs on secondary
        success = False
        for i in range(60):
            with self.db.cursor() as cursor:
                cursor.execute("select * from stl_wlm_multi_az_routing "
                               "where routing_decision = 1 "
                               "and multi_az_used_memory > 0 "
                               "order by recordtime desc")
                rows = cursor.fetchall()
                if len(rows) > 0:
                    log.info("Last row: {}".format(rows[0]))
                    # We successfully routed to multiAZ
                    last_row = rows[0]
                    main_total_memory = last_row[5]
                    multi_az_total_memory = last_row[7]
                    multi_az_used_memory = last_row[8]
                    assert multi_az_total_memory > 0
                    assert multi_az_used_memory > 0
                    # Note: this is true since both clusters
                    # have a single queue
                    assert main_total_memory == multi_az_total_memory
                    success = True
                    break
                else:
                    # Print debugging info about the cluster
                    cursor.execute(
                        "select * from stv_burst_manager_cluster_info")
                    d_rows = cursor.fetchall()
                    log.info("Cluster status: {}".format(d_rows))
                    # Print debugging info about inflight queries
                    cursor.execute(""" select userid, pid, query, starttime,
                               concurrency_scaling_status,
                               btrim(text)
                               from stv_inflight """)
                    d_rows = cursor.fetchall()
                    for d_row in d_rows:
                        log.info("Inflight query: {}".format(d_row))
                    # Print debugging info about WLM decisions
                    cursor.execute(""" select pid, query, recordtime,
                        main_total_memory,
                        main_used_memory,
                        multi_az_total_memory,
                        multi_az_used_memory,
                        routing_threshold,
                        routing_decision
                        from stl_wlm_multi_az_routing
                        order by recordtime desc
                        limit 20 """)
                    d_rows = cursor.fetchall()
                    for d_row in d_rows:
                        log.info("Decision: {}".format(d_row))

            time.sleep(1)

        assert success, "No queries routed to multiAZ"
