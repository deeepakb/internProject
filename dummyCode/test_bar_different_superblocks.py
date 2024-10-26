# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import random
import os
import shutil
import raff.s3commit.crash_events as crash_events
import raff.s3commit.defaults as s3commit_defaults
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite

MIN_ROWS = 3
TBL_NAME = "bar_tests"


# JDBC will fail for this test, SIM to track investigation on that:
# https://redshift-gerrit.corp.amazon.com/c/redshift-padb/+/111744.
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBARDifferentSuperblocks(BARTestSuite):
    """
    Test to verify that backup passes after padb have crashed in the middle of
    commit phase 1. Adopted from s3commit/test_fault_injections_basic.py
    1. Load data.
    2. Crash padb with xen_guard.
    3. Restart padb.
    4. Backup.
    5. Verify backup passes.
    """

    def setup_method(self, method):
        if not os.path.exists(s3commit_defaults.XEN_GUARD_DIR):
            os.makedirs(s3commit_defaults.XEN_GUARD_DIR)

    def teardown_method(self, method):
        if os.path.exists(s3commit_defaults.XEN_GUARD_DIR):
            shutil.rmtree(s3commit_defaults.XEN_GUARD_DIR)

    @classmethod
    def modify_test_dimensions(cls):
        # Need only backup_restore format v2 and backup_sb_format v1
        gucs = BARTestSuite.backup_restore_configs_v2()
        gucs['backup_sb_format_restore_full_sb'] = [('1', 'false')]

        return Dimensions(gucs)

    def test_bar_different_sb(self, cluster_session,
                              cluster, vector):
        # The event used to crash padb during commit.
        crash_event = crash_events.CRASH_EVENTS[
            "fdisk_process_message_commit_p1_node_0"]
        # The backup_restore gucs.
        custom_gucs = self.get_guc_dict_from_dimensions(vector)
        # The s3commit gucs.
        custom_gucs.update(s3commit_defaults.S3COMMIT_ENABLED_GUCS_LOCALHOST)

        with cluster_session(gucs=custom_gucs,
                             clean_db_before=True, clean_db_after=True):
            # Prepare data.
            with self.db.cursor() as cursor:
                self.create_table_insert_random_rows(cursor, MIN_ROWS)

            # Verify that padb will block once event is set, then restart cluster
            crash_event.crash_on_event(
                cluster,
                "INSERT INTO {} VALUES ({}, {}, {}, {});".format(
                    TBL_NAME, random.random(), random.random(),
                    random.random(), random.random()),
                db_session=self.db)

            # Take another backup after restart, verify it finishes successfully.
            self.take_backup()
