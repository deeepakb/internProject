# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode_method,\
    get_burst_conn_params, s3_commit_personalize_ss_mode
from raff.burst.burst_write import BurstWriteTest
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession

__all__ = [super_simulated_mode_method]

SELECT_STMT = "select * from {}"
TABLE_ID_QUERY = "select 'public.{}'::regclass::oid;"
CHECK_MAIN_BACKUP = "select count(*) from stl_backup_leader"
CHECK_GRAFTED_TABLE = "select count(*) from stv_tbl_perm where id = {};"
CHECK_BURST_PERSONALIZATION = \
        "select refreshed_version, is_commit_based_cold_start, personalization_type \
        from stl_burst_manager_personalization;"

BURST_MODE = """
set query_group to burst;
"""


def get_unification_q_count(cluster):
    QUERY = ("select count(*) from stl_datasharing_log "
             "where message = 'Burst uses data sharing localization' "
             "and userid > 1")
    with RedshiftDb(conn_params=cluster.get_conn_params()) as db:
        with db.cursor() as cursor:
            cursor.execute(QUERY)
            count = int(cursor.fetch_scalar())
    return count


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_tpcds("call_center")
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(gucs=burst_unified_remote_exec_gucs_main())
class TestBurstDSUnifyPersonalization(BurstWriteTest):
    """
    Test suite to Burst-DS personalization without backup
    """

    def _check_no_backup(self, cursor):
        '''
        Helper function to check no backup is uploaded to s3
        '''
        cursor.execute(CHECK_MAIN_BACKUP)
        assert cursor.fetch_scalar() == 0, "Backup is taken!"

    def _check_no_table_grafting_burst(self, tbl_ids):
        '''
        Helper function to check no table is grafed in burst cluster
        '''
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            for id in tbl_ids:
                burst_cursor.execute(CHECK_GRAFTED_TABLE.format(id))
                assert burst_cursor.fetch_scalar(
                ) == 0, "Table {} is grafted!".format(id)

    def _check_burst_cluster_refresh_version(self, cursor):
        '''
        Helper function to check burst cluster refresh version.
        With unification code, burst cluster refreshed version = -1,
        is_commit_based_cold_start = 1 (true),
        personalization_type = kBurstUseDsLocalization (3)
        '''
        cursor.execute(CHECK_BURST_PERSONALIZATION)
        # Expected value refreshed version = -1, is_commit_based_cold_start = 1
        # is_unification_path = 3
        col_name = [
            "refreshed_version", "is_commit_based_cold_start",
            "is_unification_path"
        ]
        expected_value = [-1, 1, 3]
        result = cursor.fetchall()[0]
        for i in range(len(col_name)):
            assert str(result[i]) == str(expected_value[i]), \
                  "Expected {} value: {}, exact value: {}.".format(
                      col_name[i], expected_value[i], result[i])

    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_ds_unification_personaliztion(self, cluster):
        '''
        Test personalization with s3 commit and Burst-DS Unification
        is independent to backup, and burst ready query goes with
        Burst-DS path.
        Simulated Burst cluster needs to be freed after test to
        avoid burst_acquire failure in repeated run.
        '''
        table_names = ['call_center']
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            # Check we don't take any backup
            self._check_no_backup(bs_cursor)
            # Acquire burst cluster and personalize with s3 commit
            s3_commit_personalize_ss_mode(cluster)
            self._check_burst_cluster_refresh_version(bs_cursor)
            # Check if any table is grafted during personalization
            tbl_ids = []
            for tbl_name in table_names:
                bs_cursor.execute(TABLE_ID_QUERY.format(tbl_name))
                tbl_ids.append(bs_cursor.fetch_scalar())
            self._check_no_table_grafting_burst(tbl_ids)
            # Validate burst read queries is bursted and goes
            # Burst-DS path
            unification_before = get_unification_q_count(cluster)
            cursor.execute(BURST_MODE)
            for tbl_name in table_names:
                cursor.execute(SELECT_STMT.format(tbl_name))
                self._check_last_query_bursted(cluster, cursor)
            unification_after = get_unification_q_count(cluster)
            assert unification_after == \
                unification_before + len(table_names), \
                "Some queries did not use unified path"
