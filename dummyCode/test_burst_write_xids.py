# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import random

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'enable_burst_failure_handling': 'true'
})
class TestBurstWriteXids(BurstWriteTest):
    def burst_insert_query_and_get_xid(self, cluster, cursor):
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        while True:
            random_int = random.randint(0, 100)
            cursor.execute("SELECT xid from stl_query where querytxt ilike \
                        \'%INSERT INTO test_table VALUES ({})%\';".format(random_int))
            already_inserted = cursor.fetchone()
            if already_inserted is None:
                break
        cursor.execute("INSERT INTO test_table VALUES ({});".format(random_int))
        cursor.execute("SELECT xid from stl_query where querytxt ilike \
                        \'%INSERT INTO test_table VALUES ({})%\';".format(random_int))
        result = cursor.fetchone()
        if result is not None:
            xid = result[0]
            log.info("value retrieved: {}".format(xid))
        else:
            raise Exception("No burst write xid found.")
        return xid

    def test_burst_write_xids(self, cluster):
        """
        This test creates a simple table and starts a transaction. Afterwards,
        we burst a simple insert query and check to see if the xid has been
        added to stl_burst_write_transaction with status 1. We then commit the
        transaction and check if stl_burst_write_transaction for the xid shows
        status 2. Next, we repeat the above steps, but abort instead of commit,
        and check if stl_burst_write_transaction for the xid shows status 3.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            # Creating a simple table to run our tests.
            cursor.execute("CREATE TABLE test_table (id int) diststyle even;")
            # This starts a transaction block.
            cursor.execute("BEGIN;")
            # Here we are attempting to burst a write query in the transaction.
            xid = self.burst_insert_query_and_get_xid(cluster, cursor)
            # Here, we are checking to see if the xid was recorded with a status=0.
            boot_cursor.execute("SELECT count(status) FROM \
                                stl_burst_write_transactions \
                                WHERE xid={} AND status=0;".format(xid))
            assert boot_cursor.fetchone()[0] == 1
            # Here we are committing the transaction and checking to see if the xid
            # was recorded with status=1.
            cursor.execute("COMMIT;")
            boot_cursor.execute("SELECT count(status) FROM \
                                stl_burst_write_transactions \
                                WHERE xid={} AND status=1;".format(xid))
            assert boot_cursor.fetchone()[0] == 1
            # Repeating the steps above, but aborting instead of committing.
            cursor.execute("BEGIN;")
            xid = self.burst_insert_query_and_get_xid(cluster, cursor)
            cursor.execute("ABORT;")
            boot_cursor.execute("SELECT count(status) FROM \
                                stl_burst_write_transactions \
                                WHERE xid={} AND status=2;".format(xid))
            assert boot_cursor.fetchone()[0] == 1
