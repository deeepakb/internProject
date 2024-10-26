# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from test_burst_refresh_only_changed_tables import (
    BaseBurstRefreshChangedTables, ENABLE_CBR_BURST_SCM_GUCS, INSERT_VALUES,
    DELETE_VALUES)
from raff.common.db.session import DbSession
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstSkipRefreshEmptyChangedTbls(BaseBurstRefreshChangedTables):
    def test_burst_skip_refresh_empty_changed_tbls(self, cluster):
        """
        This test verifies a corner case that should not block new burst query:
        1. Run a DELETE query that will not delete anything from the table.
        2. The DELETE query will not mark the table as changed table in sb
           commit. So the new refresh will be skipped if the table is the only
           one to be refreshed.
        3. But the table's dml_end_version_ will be updated after the DELETE.
        4. New burst query on the table should not be blocked.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            num_tbls = 3
            self._setup_tables(cursor, num_tbls)
            self._start_and_wait_for_refresh(cluster)
            # validate table content after tables setup
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            changed_tbls = set()
            skipped_tbls = set()
            tbl_name_raw = 'dp48783_tbl_{}'
            tbl_idx = -1

            # case 0: DELETE Nothing
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(DELETE_VALUES.format(tbl_idx))
            skipped_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # Refresh will be skipped since no tbl is actually changed
            refresh_status = self._start_and_wait_for_refresh(cluster)
            assert refresh_status == "RefreshClusterAlreadyUpToDate"
            # Trigger a write and the DELETE nothing table needs to be skipped
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))
            # New refresh would not be skipped
            self._start_and_wait_for_refresh(cluster)
            burst_skipped_tbls = self._get_latest_skipped_tbl_list()
            burst_changed_tbls = self._get_latest_changed_tbl_list()
            log.info("burst skipped list: {}".format(
                sorted(burst_skipped_tbls)))
            log.info("burst changed list: {}".format(
                sorted(burst_changed_tbls)))
            self._validate_tables_list(skipped_tbls, burst_skipped_tbls)
            self._validate_tables_list(changed_tbls, burst_changed_tbls)
            for i in range(num_tbls):
                cursor.execute("set query_group to burst;")
                cursor.execute(INSERT_VALUES.format(i))
                self._check_last_query_bursted(cluster, cursor)
            self._validate_all_tables_content(cluster, cursor, num_tbls)
