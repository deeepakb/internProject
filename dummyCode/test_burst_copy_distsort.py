# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_temp_write import (BurstTempWrite, burst_user_temp_support_gucs)
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


class BaseBurstCopyDistSort(BurstTempWrite):
    def base_test_basic_burst_copy_all_dist_sort(self, cluster, vector,
                                                 is_temp, unified_remote_exec):
        """
        Test: run burst copy on table with different distsyle and sortkey.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            schema = db_session.session_ctx.schema
            self._setup_table(
                db_session,
                schema,
                'catalog_returns',
                'tpcds',
                '1',
                vector.diststyle,
                vector.sortkey,
                '_burst',
                is_temp=is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor,
                                                     "catalog_returns_burst")
            self._start_and_wait_for_refresh(cluster)

            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.execute("select count(*) from catalog_returns_burst;")
            catalog_returns_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1

            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            # COPY should be bursted since burst cluster is refreshed.
            cursor.run_copy(
                'catalog_returns_burst',
                's3://tpc-h/tpc-ds/1/catalog_returns.',
                gzip=True,
                delimiter="|",
                COMPUPDATE="OFF")
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_query_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            catalog_returns_size = catalog_returns_size + copy_rows

            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            # Read only query should be bursted since PADB support burst read
            # query on burst cluster just handle write.
            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size
            self._check_last_query_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            self._validate_table(cluster, schema, 'catalog_returns_burst',
                                 vector.diststyle)

            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            # Next copy and select should still be able to burst.
            cursor.run_copy(
                'catalog_returns_burst',
                's3://tpc-h/tpc-ds/1/catalog_returns.',
                gzip=True,
                delimiter="|",
                COMPUPDATE="OFF")
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_query_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            catalog_returns_size = catalog_returns_size + copy_rows

            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size
            self._check_last_query_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            self._validate_table(cluster, schema, 'catalog_returns_burst',
                                 vector.diststyle)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyDistSortSS(BaseBurstCopyDistSort):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['even', 'key distkey cr_returned_date_sk'],
                sortkey=['', 'cr_item_sk']))

    def test_basic_burst_copy_all_dist_sort(self, cluster, vector, is_temp):
        self.base_test_basic_burst_copy_all_dist_sort(
            cluster, vector, is_temp, unified_remote_exec=False)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
class TestBurstCopyDistSortCluster(BaseBurstCopyDistSort):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['even', 'key distkey cr_returned_date_sk'],
                 sortkey=['', 'cr_item_sk']))

    def test_basic_burst_copy_all_dist_sort(self, cluster, vector):
        self.base_test_basic_burst_copy_all_dist_sort(cluster, vector, is_temp=False,
                                                      unified_remote_exec=False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
class TestBurstCopySSModeDistSortSSUnifiedRemoteExec(
        BaseBurstCopyDistSort):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['even', 'key distkey cr_returned_date_sk'],
                sortkey=['', 'cr_item_sk']))

    def test_basic_burst_copy_all_dist_sort_unified_remote_exec(
            self, cluster, vector):
        self.base_test_basic_burst_copy_all_dist_sort(
            cluster, vector, is_temp=False, unified_remote_exec=True)
