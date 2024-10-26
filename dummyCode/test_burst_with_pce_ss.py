# Copyright 2024 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.

import getpass
import logging
import uuid
import pytest

from raff.burst.burst_super_simulated_mode_helper \
    import super_simulated_mode, get_burst_conn_params
from raff.burst.burst_test import BurstTest
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)

__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('customer')
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstWithPCE_SSM(BurstTest):
    """
    Test that burst cluster uses precompiled executors.
    """

    def test_burst_works_with_pce(self, cluster, db_session):
        if cluster.get_guc_value('can_use_precompiled_executor') != 'on':
            pytest.skip("This test requires PCE enabled")

        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with db_session.cursor() as cursor, \
             burst_session.cursor() as burst_cursor:

            # Execute a burst'able query over a local TPC-DS table.
            cursor.execute("""
              set query_group to burst;
              select count(*) from customer A;
            """)

            query_id = cursor.last_query_id()
            log.info("Query ID on local cluster: {}".format(query_id))

            # Find the last query on the burst cluster
            burst_cursor.execute("""
              select concurrency_scaling_query
              from stl_concurrency_scaling_query_mapping
              where primary_query = {}
            """.format(query_id))
            burst_query_id = burst_cursor.fetch_scalar()
            log.info("Query ID on burst cluster: {}".format(burst_query_id))

            # Fetch the PCE segments compilation info
            burst_cursor.execute("""
              with cinfo as (
                select path
                from stl_compile_info
                where query = {0} and cachehit = 13
              union all
                select path
                from stl_bkg_compile_info
                where query = {0} and cachehit = 13)

              select count(distinct path) from cinfo
            """.format(burst_query_id))
            num_of_pce_segs = int(burst_cursor.fetch_scalar())

            assert num_of_pce_segs > 0, (
                "With PCEs enabled, the number of PCE segments should be a "
                "positive number.")
