# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
from raff.common.simulated_helper import wait_for_rerepp
from raff.common.cluster.cluster_session import ClusterSession
from raff.backup_restore.bar_test import BARTestSuite


@pytest.mark.serial_only
class TestReplaceCompute(BARTestSuite):

    def test_replace_compute(self, cluster):
        """
        Test Verifies compute node replacement by creating data and
        setting a specified event to slow down the rerep process and 
        check the data before and after the event after reboot.
        """
        with ClusterSession(cluster)(clean_db_before=True,
                                     clean_db_after=True):
            with self.db.cursor() as cursor:
                # create tables and add data
                cursor.execute("create table bar_test1 \
                               (a bigint, b bigint);")
                cursor.execute("insert into bar_test1 values (1, 1), \
                               (1, 2), (1, 3);")
                # slow down re-replication so the query fetches
                # data from the mirror node during re-rep
                cluster.set_events_in_padb_conf(
                    ['EtDelayFDiskIO,rerep_read=10000'])
                cluster.reboot_cluster()
                with self.db.cursor() as cursor:
                    # do query
                    cursor.execute("select * from bar_test1 values;")
                    cluster.run_xpx('event unset EtDelayFDiskIO')
                    wait_for_rerepp()
                    # run the query again
                    cursor.execute("select * from bar_test1 values;")
                    cursor.execute("select count(*) from bar_test1;")
                    assert cursor.fetch_scalar() == 3
