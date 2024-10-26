# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import time

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest, \
        burst_write_throttle_disable_gucs, burst_write_throttle_enable_gucs
import raff.s3commit.db_helpers as db_helpers

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, burst_write_throttle_disable_gucs]
burst_cluster_gucs = {'burst_enable_write': 'true'}

table_def = """create table {} (
                c_custkey int8 not null encode delta,
                c_name varchar(max) null encode lzo,
                c_address varchar(max) not null encode lzo,
                c_nationkey int4 null encode delta,
                c_phone char(15) not null encode lzo,
                c_acctbal numeric(12,2) not null encode lzo,
                c_mktsegment char(10) not null encode bytedict,
                c_comment varchar(max) null encode zstd
                ) diststyle even"""
copy_stmt = """ copy {} from 's3://tpc-h/1/customer.tbl' CREDENTIALS
                'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3'
                gzip delimiter '|';"""
insert_small_stmt = "insert into {} (select * from {} limit 10);"
insert_select_stmt = "insert into {} (select * from {});"


class BaseBurstWriteThrottle(BurstWriteTest):
    def _test_throttle_on_big_insert(
            self, cursor, bootstrap_cursor, cluster, table, is_burst):
        if is_burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        # Verify that small and medium commits are not throttled.
        cursor.execute(insert_small_stmt.format(table, table))
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)
        throttles = db_helpers.get_max_and_min_delete_tag_throttle(
            bootstrap_cursor)
        assert len(throttles) == 0

        # Medium commits (~400 blocks per node) do not incur
        # throttling as well.
        cursor.execute("begin;")
        for i in range(2):
            cursor.execute(insert_select_stmt.format(table, table))
            if is_burst:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("commit;")
        throttles = db_helpers.get_max_and_min_delete_tag_throttle(
            bootstrap_cursor)
        assert len(throttles) == 0

        # Large commits will incur throttling.
        cursor.execute("begin;")
        for i in range(8):
            cursor.execute(insert_select_stmt.format(table, table))
            if is_burst:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("commit;")

        throttles = db_helpers.get_max_and_min_delete_tag_throttle(
            bootstrap_cursor)
        if is_burst:
            # Verify that the maximum outstanding delete tags are
            # reduced.
            assert throttles[0][0] > throttles[0][1]
        else:
            assert len(throttles) == 0

    def _setup_tables(self, cluster, cursor, table):
        cursor.execute(table_def.format(table))
        cursor.execute(copy_stmt.format(table))
        self._start_and_wait_for_refresh(cluster)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(
        gucs={'s3commit_enable_delete_tag_throttling': 'false',
              'max_unthrottled_delete_tags_per_cluster': 1500,
              'min_unthrottled_delete_tags_per_cluster': 64,
              'max_tps_delete_tags': 128,
              'delete_tags_time_window': 1,
              's3_write_non_fatal_threshold_for_critical': 100,
              'burst_write_enable_delete_tag_throttling': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_cluster_gucs)
class TestBurstWriteThrottleDisable(BaseBurstWriteThrottle):
    def test_burst_write_commit_throttling(self, cluster, db_session):
        """
        Verify burst write query trigger delete tag throttle when delete
        tag throttle guc is disable.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_one")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_one", True)

    def test_non_burst_write_commit_throttle(self, cluster, db_session):
        """
        Verify non-burst-write query doesn't trigger delete tag throttle
        when delete tag throttle guc is disable
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_two")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_two", False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(
        gucs={'s3commit_enable_delete_tag_throttling': 'true',
              'max_unthrottled_delete_tags_per_cluster': 1500,
              'min_unthrottled_delete_tags_per_cluster': 64,
              'max_tps_delete_tags': 128,
              'delete_tags_time_window': 1,
              's3_write_non_fatal_threshold_for_critical': 100,
              'burst_write_enable_delete_tag_throttling': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_cluster_gucs)
class TestBurstWriteThrottleEnable(BaseBurstWriteThrottle):
    def test_burst_write_commit_throttling(self, cluster, db_session):
        """
        Verify burst write query trigger delete tag throttle when delete
        tag throttle guc is enabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_one")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_one", True)

    def test_non_burst_write_commit_throttle(self, cluster, db_session):
        """
        Verify non-burst-write query doesn't trigger delete tag throttle
        when delete tag throttle guc is enabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_two")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_two", True)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_local_gucs(
        gucs={'s3commit_enable_delete_tag_throttling': 'false',
              'max_unthrottled_delete_tags_per_cluster': 1500,
              'min_unthrottled_delete_tags_per_cluster': 64,
              'max_tps_delete_tags': 128,
              'delete_tags_time_window': 1,
              's3_write_non_fatal_threshold_for_critical': 100,
              'burst_write_enable_delete_tag_throttling':'false'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_cluster_gucs)
class TestBurstWriteThrottleBothDisable(BaseBurstWriteThrottle):
    def test_burst_write_commit_no_throttling(self, cluster, db_session):
        """
        Verify burst write query doesn't trigger delete tag throttle when
        delete tag throttle guc is disabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_one")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_one", False)

    def test_non_burst_write_commit_no_throttle(self, cluster, db_session):
        """
        Verify non-burst-write query doesn't trigger delete tag throttle
        when delete tag throttle guc is disabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_two")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_two", False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(
        gucs={'s3commit_enable_delete_tag_throttling': 'true',
              'max_unthrottled_delete_tags_per_cluster': 1500,
              'min_unthrottled_delete_tags_per_cluster': 64,
              'max_tps_delete_tags': 128,
              'delete_tags_time_window': 1,
              's3_write_non_fatal_threshold_for_critical': 100,
              'burst_write_enable_delete_tag_throttling':'false'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_cluster_gucs)
class TestBurstWriteThrottleS3CommitEnable(BaseBurstWriteThrottle):
    def test_burst_write_commit_throttling(self, cluster, db_session):
        """
        Verify burst write query doesn't trigger delete tag throttle when
        delete tag throttle guc is disabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_one")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_one", True)

    def test_non_burst_write_commit_throttle(self, cluster, db_session):
        """
        Verify non-burst-write query doesn't trigger delete tag throttle
        when delete tag throttle guc is disabled.
        """
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            self._setup_tables(cluster, cursor, "table_two")
            self._test_throttle_on_big_insert(
                    cursor, bootstrap_cursor, cluster, "table_two", True)
