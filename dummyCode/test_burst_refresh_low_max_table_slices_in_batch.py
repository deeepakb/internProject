# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.burst.burst_super_simulated_mode_helper import (super_simulated_mode,
                                                          cold_start_ss_mode)
from raff.superblock.helper import get_tree_based_dual_path_superblock_gucs
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.simulated_helper import create_localhost_snapshot

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

ENABLE_CBR_CTR_SCM = {
    'burst_refresh_max_table_slices_in_batch': '2',
    'enable_burst_refresh_changed_tables': 'true',
    'enable_burst_s3_commit_based_refresh': 'true',
    'burst_use_ds_localization': 'false',
    'enable_burst_s3_commit_based_cold_start': 'false'
}

DISABLE_CBR_CTR_SCM = {
    'burst_refresh_max_table_slices_in_batch': '2',
    'enable_burst_refresh_changed_tables': 'false',
    'enable_burst_s3_commit_based_refresh': 'false',
    'burst_refresh_prefer_epsilon_metadata': 'false',
    'burst_refresh_prefer_epsilon_superblock': 'false',
    'burst_use_ds_localization': 'false',
    'enable_burst_s3_commit_based_cold_start': 'false'
}
DISABLE_CBR_CTR_SCM.update(get_tree_based_dual_path_superblock_gucs())


class BaseBurstRefreshLowMaxTableSlicesInBatch(BurstWriteTest):
    def _setup_tables(self, db_session, schema):
        diststyle_one = 'key distkey cr_returned_date_sk'
        diststyle_two = 'key distkey cc_call_center_id'
        sortkey_one = 'cr_item_sk'
        sortkey_two = 'cc_call_center_id'
        for idx in range(5):
            self._setup_table(db_session, schema, 'catalog_returns', 'tpcds',
                              '1', diststyle_one, sortkey_one,
                              '_burst_' + str(idx))
            self._setup_table(db_session, schema, 'call_center', 'tpcds', '1',
                              diststyle_two, sortkey_two, '_burst_' + str(idx))

    def _validate_tables(self, cluster, cursor, bs_cursor):
        count_star = "select count(*) from {}_burst_{};"
        for idx in range(5):
            bs_cursor.execute(count_star.format("catalog_returns", idx))
            count_main = bs_cursor.fetch_scalar()
            cursor.execute(count_star.format("catalog_returns", idx))
            count = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            assert count == count_main
            bs_cursor.execute(count_star.format("call_center", idx))
            count_main = bs_cursor.fetch_scalar()
            cursor.execute(count_star.format("call_center", idx))
            count = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            assert count == count_main

    def _insert_tables(self, bs_cursor):
        insert_select = "insert into {}_burst_{} select * from {}_burst_{} limit 1;"
        for idx in range(5):
            bs_cursor.execute(
                insert_select.format("catalog_returns", idx, "catalog_returns",
                                     idx))
            bs_cursor.execute(
                insert_select.format("call_center", idx, "call_center", idx))

    def burst_refresh_low_max_table_slices_in_batch(self, cluster):
        '''
        Test workload:
        1. Create 10 tables and load data into them.
        2. Trigger coldstart and validate the tables contents are the same
           across main and burst.
        3. Insert data into all the tables.
        4. Trigger burst refresh and validate the tables contents are the same
           across main and burst.
        '''
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            bs_cursor.execute("set query_group to noburst;")
            bs_cursor.execute(
                'SET SEARCH_PATH TO {}, "$user", public;'.format(schema))
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            create_localhost_snapshot(
                "backup_{}".format(uuid.uuid4()), wait=True)
            cold_start_ss_mode(cluster)
            self._validate_tables(cluster, cursor, bs_cursor)
            self._insert_tables(bs_cursor)
            self._start_and_wait_for_refresh(cluster, refresh_wait_time=60)
            self._validate_tables(cluster, cursor, bs_cursor)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(ENABLE_CBR_CTR_SCM, **{
        'udf_start_lxc': 'false',
    }))
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_CTR_SCM)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstRefreshLowMaxTableSlicesInBatchCTR(
        BaseBurstRefreshLowMaxTableSlicesInBatch):
    def test_burst_refresh_low_max_table_slices_in_batch(self, cluster):
        self.burst_refresh_low_max_table_slices_in_batch(cluster)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(DISABLE_CBR_CTR_SCM, **{
        'udf_start_lxc': 'false',
    }))
@pytest.mark.custom_local_gucs(gucs=DISABLE_CBR_CTR_SCM)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstRefreshLowMaxTableSlicesInBatchNoCTR(
        BaseBurstRefreshLowMaxTableSlicesInBatch):
    def test_burst_refresh_low_max_table_slices_in_batch(self, cluster):
        self.burst_refresh_low_max_table_slices_in_batch(cluster)
