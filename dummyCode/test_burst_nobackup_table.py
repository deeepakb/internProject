# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import getpass
import logging
import uuid
import json
import pytest
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.base_test import run_priviledged_query
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [super_simulated_mode, setup_teardown_burst]
log = logging.getLogger(__name__)
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TABLE = "create table if not exists no_backup_table( \
                test_text varchar(255) not null) \
                BACKUP NO \
                diststyle even"

INSERT = "INSERT INTO {} VALUES ('{}')"
SELECT = "SELECT * FROM no_backup_table"


class NoBackUpTable(BurstWriteTest):
    def _setup_no_backup_table(self, cursor):
        cursor.execute(CREATE_TABLE)
        cursor.execute(INSERT.format("no_backup_table", "first insert"))
        cursor.execute(INSERT.format("no_backup_table", "second insert"))
        for i in range(25):
            cursor.execute("INSERT INTO no_backup_table {}".format(SELECT))

    def _execute_burst_write_query(self, cursor, table):
        cursor.execute("set selective_dispatch_level = 0;")
        cursor.execute("set enable_result_cache_for_session to off;")
        cursor.execute(INSERT.format(table, "customer_burst"))

    def _burst_nobackup_table_S3CommitOn(self, cluster, cursor, boot_cursor):
        """
        This test verifies that we should be able to run burst read and
        write queries with s3 commit and burst write being enabled.
        """
        cursor.execute('set query_group to burst')
        self._setup_no_backup_table(cursor)
        self._start_and_wait_for_refresh(cluster)
        cursor.execute(SELECT)
        self._check_last_query_bursted(cluster, cursor)
        self._execute_burst_write_query(cursor, "no_backup_table")
        self._check_last_query_bursted(cluster, cursor)

    def _burst_nobackup_table_S3CommitDisabled(self, cluster, cursor,
                                               boot_cursor):
        """
        This test verifies that we should not be able to run burst read and
        write queries with s3 commit and burst write being disabled.
        """
        cursor.execute('set query_group to burst')
        self._setup_no_backup_table(cursor)
        self._start_and_wait_for_refresh(cluster)
        cursor.execute(SELECT)
        self._check_last_query_didnt_burst(cluster, cursor)
        self._execute_burst_write_query(cursor, "no_backup_table")
        self._check_last_query_didnt_burst(cluster, cursor)

    def burst_no_backup_table_helper(self, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            guc_query_s3_mirror = ("show enable_mirror_to_s3;")
            guc_query_commits_to_dynamo = ("show enable_commits_to_dynamo;")
            guc_res_s3_mirror = run_priviledged_query(cluster, boot_cursor,
                                                      guc_query_s3_mirror)
            guc_res_commits_to_dynamo = run_priviledged_query(
                cluster, boot_cursor, guc_query_commits_to_dynamo)
            burst_s3_commit_enabled = guc_res_s3_mirror[0][0] == '2' \
                and guc_res_commits_to_dynamo[0][0] == '2'
            if burst_s3_commit_enabled:
                self._burst_nobackup_table_S3CommitOn(cluster, cursor,
                                                      boot_cursor)
            else:
                self._burst_nobackup_table_S3CommitDisabled(
                    cluster, cursor, boot_cursor)
        db_session.close()


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh': 'false',
        'enable_burst_s3_commit_based_cold_start': 'false',
        'enable_burst_refresh_changed_tables': 'false',
        'burst_enable_write': 'true',
        'enable_mirror_to_s3': '2',
        'enable_commits_to_dynamo': '2'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh': 'false',
        'enable_burst_s3_commit_based_cold_start': 'false',
        'enable_burst_refresh_changed_tables': 'false',
        'burst_enable_write': 'true',
        'enable_mirror_to_s3': '2',
        'enable_commits_to_dynamo': '2',
        'enable_burst_async_acquire': 'false'
    })
class TestNoBackUpTableS3CommitOn(NoBackUpTable):
    def test_burst_no_backup_table_s3commit_on(self, cluster):
        self.burst_no_backup_table_helper(cluster)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh': 'false',
        'enable_burst_s3_commit_based_cold_start': 'false',
        'enable_burst_refresh_changed_tables': 'false',
        'burst_enable_write': 'false',
        'enable_mirror_to_s3': '0',
        'enable_commits_to_dynamo': '0'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh': 'false',
        'enable_burst_s3_commit_based_cold_start': 'false',
        'enable_burst_refresh_changed_tables': 'false',
        'burst_enable_write': 'false',
        'enable_mirror_to_s3': '0',
        'enable_commits_to_dynamo': '0',
        'enable_burst_async_acquire': 'false'
    })
class TestNoBackUpTableS3CommitDisabled(NoBackUpTable):
    def test_burst_no_backup_table_s3commit_off(self, cluster):
        self.burst_no_backup_table_helper(cluster)


CUSTOM_ENABLE_GUCS = dict({
    'enable_burst_s3_commit_based_refresh':
    'false',
    'enable_burst_s3_commit_based_cold_start':
    'false',
    'enable_burst_refresh_changed_tables':
    'false',
    'burst_enable_write':
    'true',
    'enable_mirror_to_s3':
    '2',
    'enable_commits_to_dynamo':
    '2',
    'enable_burst_async_acquire':
    'false',
    'burst_propagated_static_gucs':
    json.dumps({
        "padbGucs": [{
            "name": "enable_burst_s3_commit_based_refresh",
            "value": "false"
        }, {
            "name": "enable_burst_s3_commit_based_cold_start",
            "value": "false"
        }, {
            "name": "burst_enable_write",
            "value": "true"
        }]
    })
})


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.skip_load_data
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_ENABLE_GUCS)
class TestNoBackUpTableS3CommitOnCluster(NoBackUpTable):
    @pytest.mark.skip(reason="RedshiftDP-71019")
    def test_burst_no_backup_table_s3commit_on(self, cluster):
        self.burst_no_backup_table_helper(cluster)


CUSTOM_DISABLE_GUCS = dict({
    'enable_burst_s3_commit_based_refresh':
    'false',
    'enable_burst_s3_commit_based_cold_start':
    'false',
    'enable_burst_refresh_changed_tables':
    'false',
    'burst_enable_write':
    'false',
    'enable_burst_async_acquire':
    'false',
    'enable_mirror_to_s3':
    '0',
    'enable_commits_to_dynamo':
    '0',
    'burst_propagated_static_gucs':
    json.dumps({
        "padbGucs": [{
            "name": "enable_burst_s3_commit_based_refresh",
            "value": "false"
        }, {
            "name": "enable_burst_s3_commit_based_cold_start",
            "value": "false"
        }, {
            "name": "burst_enable_write",
            "value": "false"
        }]
    })
})


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.skip_load_data
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_DISABLE_GUCS)
class TestNoBackUpTableS3CommitDisabledCluster(NoBackUpTable):
    @pytest.mark.skip(reason="RedshiftDP-71019")
    def test_burst_no_backup_table_s3commit_off(self, cluster):
        self.burst_no_backup_table_helper(cluster)
