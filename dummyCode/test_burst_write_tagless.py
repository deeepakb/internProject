# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import copy
import logging
import pytest
from raff.burst.burst_status import BurstStatus
from raff.burst.burst_super_simulated_mode_helper import (
    LOCALHOST_GUCS, get_burst_conn_params,
    get_burst_tagless_wam_files_on_burst, get_guids_from_tagless_wams_on_burst,
    get_guids_from_tagless_wams_on_main)
from raff.common.db.redshift_db import RedshiftDb
from test_burst_write_basic import (
    TestBurstWriteBasicBase, MY_SCHEMA, MY_USER)
from raff.common.aws_clients.s3_client import S3Client
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.profile import Profiles
from raff.s3commit import db_helpers, defaults, helpers


log = logging.getLogger(__name__)

INSERT = "INSERT INTO {} VALUES {}"

# For main cluster, combine the retry and tagless gucs
burst_local_gucs = copy.deepcopy(defaults.BURST_WRITE_RETRY_GUCS)
burst_local_gucs.update(defaults.BURST_TAGLESS_GUCS)

# Disable the burst tagless write guc on the burst cluster. Since it is
# enabled on main cluster, the tagless write related test validation passing
# means the guc has been propagated correctly from main to burst during
# personalization.
BURST_TAGLESS_WRITE_DISABLED = {
    's3commit_burst_tagless_perm_writes_mode': '0',
}


@pytest.mark.precommit
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=BURST_TAGLESS_WRITE_DISABLED)
@pytest.mark.custom_local_gucs(gucs=burst_local_gucs)
class TestBurstTaglessWrites(TestBurstWriteBasicBase):
    """
    Class for testing Burst Tagless Writes.
    """

    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_tagless_writes_basic(self, cluster):
        """
        Basic test for Burst Tagless Write on Mode 2 (full burst tagless mode)
        1. Create table and perform Burst refresh
        2. Burst an insert query
        3. Verify S3 writes are Tagless
        4. Verify no DeleteTags on Main after query completion
        5. Compare all the guids in the WAMs on burst with the table guids
           (stv_blocklist)
        6. Compare all the guids in the WAMs on main with the table guids
           (stv_blocklist)
        """
        test_gucs = copy.deepcopy(LOCALHOST_GUCS)
        # This just sets the necessary gucs for Tag checker, not really
        # used by the test elsewhere.
        test_gucs.update({
            's3_backup_block_midfix':
                cluster.get_guc_value('s3_backup_block_midfix'),
            's3_backup_key_prefix':
                cluster.get_guc_value('s3_backup_key_prefix'),
        })
        # Get a non-bootstrap user session on main cluster
        user_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())

        # Setup tables for burst cluster and perform a burst write insert
        with user_session.cursor() as main_cursor:
            # Set event to prevent deletion of BurstLocalWAMs that are
            # generated upon receiving headers from burst cluster and
            # typically deleted during the next commit.
            cluster.set_event('EtS3CommitTaglessTestSkipBurstWamCleanup')
            # Setup tables and perform refresh.
            table_name = self._setup_source_tables(cluster, main_cursor)[0]
            log.info("Created table %s with table_id %d", table_name,
                     db_helpers.get_table_id_from_name(self.db, table_name))
            main_cursor.execute("set enable_result_cache_for_session to off")
            main_cursor.execute("set query_group to burst")
            # Set event to prevent deletion of BurstLocalWAMs upon completion
            # of the query - yet treat the query as successful.
            with burst_db.cursor() as burst_cursor:
                # Scope of event within INSERT query is sufficient because
                # deletion will not happen again after the query is complete.
                burst_cursor.execute(
                    "xpx 'event set EtS3CommitTaglessTestSkipBurstWamCleanup'")
                main_cursor.execute(INSERT.format(table_name, self._values(1)))
            log.info("Insert query executed")
            self._check_last_query_bursted(cluster, main_cursor)
            # Read WAMs on burst cluster
            burst_wam_guids = get_guids_from_tagless_wams_on_burst()
            log.debug("Burst WAM Guids: %s", burst_wam_guids)
            # Read WAMs on Main cluster
            main_wam_guids = get_guids_from_tagless_wams_on_main()
            log.debug("Main WAM Guids: %s", main_wam_guids)

        # Validate that the block headers are in the Perm state (or in backup
        # state if for some reason a backup happens after insert and before
        # the check).
        table_guids = db_helpers.get_block_guids(self.db, table=table_name)
        log.debug("Table guids from stv_blocklist: %s", table_guids)
        db_helpers.validate_table_blocks_in_s3_state(
            self.db,
            table_name,
            s3_states=[defaults.S3STATE_PERM_MIRROR, defaults.S3STATE_BACKUP])

        # Validate that the blocks are Untagged in S3.
        s3 = S3Client(Profiles.DP_BASIC)
        helpers.validate_s3_tagging_for_blocks(
            s3, test_gucs, table_guids, cluster, is_tagged=False)

        # Fix the nesting in the output of guids and remove hyphens from
        # the guids, as the WAM tool outputs guids without the hyphen.
        table_guids = {x[0].strip().replace('-', '') for x in table_guids}
        assert burst_wam_guids == table_guids, (
            "Guids in the burst local WAM does not match the guids in the "
            "stv_blocklist for the table.")
        assert main_wam_guids == table_guids, (
            "Guids in the main local WAM does not match the guids in the "
            "stv_blocklist for the table.")

        # Validate that there is no Delete Tag operation on Main cluster
        delete_tagged_guids = db_helpers.get_guids_writes_by_type(
            self.db, s3_write_type=defaults.DELETE_TAG_WRITE,
            table_name=table_name)
        num_delete_tags_issued = len(delete_tagged_guids)
        assert num_delete_tags_issued == 0, (
            "Delete Tag operation found on burst write query. Guids: {}"
            .format(delete_tagged_guids[:min(num_delete_tags_issued, 10)]))

        log.info("Test test_burst_tagless_writes_basic completed successfully")

    @pytest.mark.usefixtures("super_simulated_mode_method")
    @pytest.mark.parametrize("wam_deletion_success", [True, False])
    def test_burst_tagless_wam_deletion(self, cluster, wam_deletion_success):
        """
        Case 1: Test for Burst Tagless WAM deletion success
        0. wam_deletion_success = True
        1. Create table and perform Burst refresh
        2. Burst an insert query
        3. Verify WAMs on burst cluster are deleted

        Case 2: Test for Burst Tagless WAM deletion failure
        0. wam_deletion_success = False
        1. Create table and perform Burst refresh
        2. Set an event on Burst that simulates WAM deletion failure
        3. Burst an insert query
        3. Verify that the query failed with the correct failure code and
        message.
        """
        log.info("Staring test_burst_tagless_wam_deletion with "
                 "wam_deletion_success {}".format(wam_deletion_success))
        # Get a non-bootstrap user session on main cluster
        user_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())

        with user_session.cursor() as main_cursor:
            # Setup tables and perform refresh.
            table_name = self._setup_source_tables(cluster, main_cursor)[0]
            log.info("Created table %s with table_id %d", table_name,
                     db_helpers.get_table_id_from_name(self.db, table_name))
            main_cursor.execute("set enable_result_cache_for_session to off")
            main_cursor.execute("set query_group to burst")
            insert_query = INSERT.format(table_name, self._values(1))

            if wam_deletion_success:
                # If the failure is not injected, WAM deletion is expected to
                # succeed and so is the query. The insert query should have
                # correct burst status.
                main_cursor.execute(insert_query)
                self._check_last_query_bursted(cluster, main_cursor)
                log.info("Insert query bursted successfully")
            else:
                # If the failure is injected, WAM deletion is expected to fail
                # and so is the query. The failed query should have the
                # expected burst query failure.
                with burst_db.cursor() as burst_cursor:
                    burst_cursor.execute(
                        "xpx 'event set "
                        "EtS3CommitTaglessTestFailBurstWamCleanup'")
                qid = self.execute_failing_query_get_query_id(
                    insert_query,
                    "Failed to delete WAMs",
                    cluster=cluster,
                    db_session=user_session)
                status = self._get_burst_status_code(cluster, qid)
                log.info("Insert query burst failed with error code {}"
                         .format(status))
                # Verify the burst error code is 87 with the failure handling
                # gucs enabled. If the guc is disabled, failure returns status
                # code 25 (i.e., failed_no_failure_handling)
                assert status == \
                    BurstStatus.burst_remote_wam_deletion_failure, (
                        "Burst query error code did not match the expected "
                        "value.")

        # Validate WAM deletion on burst cluster if no failure injected
        if wam_deletion_success:
            wam_files_on_burst = get_burst_tagless_wam_files_on_burst()
            assert len(wam_files_on_burst) == 0, (
                "Found WAMs on Burst cluster. Potential durability issue.")
        log.info("Test test_burst_tagless_wam_deletion completed successfully")
