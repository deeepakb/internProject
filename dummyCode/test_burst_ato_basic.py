# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from base_burst_alter_distkey import (BaseBurstAlterDistkey,
                                      AlterTableErrorInjectionEvents,
                                      ENABLE_BURST_ATO_NO_PERSIST_SHADOW,
                                      ENABLE_BURST_ALTER_4_SLICES,
                                      ENABLE_BURST_ALTER_8_SLICES)
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode, get_burst_conn_params)
from raff.common.db.db_exception import NotSupportedError, InternalError
from raff.common.cluster.cluster_defaults import LocalhostDefault

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstAlterDistkey(BaseBurstAlterDistkey):
    CHECK_QUERY = "select count(*), sum(c_custkey), sum(c_nationkey) from {}.{}"
    _SCHEMA = ''

    def verify_table_content(self, cursor, res):
        cmd = self.CHECK_QUERY.format(self._SCHEMA, self._TBL_NAME)
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def verify_shadow_table_on_main_and_burst(self, shadow_tbl_id):
        assert shadow_tbl_id is not None
        num_rows = "select sum(rows) from stv_tbl_perm where id = {}".format(
            shadow_tbl_id)
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(num_rows)
            burst_rows = burst_cursor.fetch_scalar()
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(num_rows)
            main_rows = bootstrap_cursor.fetch_scalar()
        assert main_rows == burst_rows, "mismatch {} {}".format(
            main_rows, burst_rows)

    def run_alter_and_validate(self, cluster, main_cursor,
                               main_bootstrap_cursor, burst_bootstrap_cursor):
        """
        Run alter distkey and validate shadow table existence and burst status.

        Args:
            cluster: Cluster object.
            main_cursor: Cursor to run alter distkey.
            main_bootstrap_cursor: Bootstrap cursor to run queries on main
                cluster.
            burst_bootstrap_cursor: Bootstrap cursor to run queries on burst
                cluster.
        """
        main_cursor.execute("alter table {} alter distkey {}".format(
            self._TBL_NAME, "c_custkey"))
        self._verify_last_alter_query_bursted(cluster)
        self.validate_row_count_matches_on_main_and_burst(
            self._TBL_NAME, main_bootstrap_cursor, burst_bootstrap_cursor)

    def test_burst_alter_distkey_basic(self, cluster):
        """
            This test create an dist-even table and simulated alter checkpoint
            1. for the first few iterations make sure all alter queries bursted
            2. after finish conversion, validate table content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self.insert_and_verify_bursted(cluster, cursor)
            cursor.execute(self.CHECK_QUERY.format(schema, self._TBL_NAME))
            res_gold = cursor.fetchall()
            self._simulate_alter_checkpoint_mode(cluster, cursor)
            shadow_tbl_id = None
            try:
                for i in range(10):
                    cursor.execute("alter table {} alter distkey {}".format(
                        self._TBL_NAME, "c_custkey"))
                    if i == 2:
                        # Trigger a refresh and make sure shadow table does not
                        # get dropped on burst, and having same state as main.
                        self._start_and_wait_for_refresh(cluster)
                        self.verify_shadow_table_on_main_and_burst(
                            shadow_tbl_id)
                    if i < 4:
                        # Verify at least some alter copy queries bursted.
                        shadow_tbl_id = self._verify_last_alter_query_bursted(
                            cluster)
            except NotSupportedError as error:
                assert 'cannot alter to the same distkey' in error.pgerror
                pass
            self._unset_checkpoint_mode(cluster, cursor)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'auto(key(c_custkey))')

    def test_burst_alter_distkey_main_own(self, cluster):
        """
        This test create an ATO-distkey table.
        1. Let it finish an alter checkpoint, make sure shadow table is owned by burst
        2. force the second iteration to no burst, verify main cluster own
        3. trigger refresh and force it to burst, verify it can not burst again
        4. Let alter finish on main verify after swap, main does not own shadow table
        5. verify table content
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self.insert_and_verify_bursted(cluster, cursor)
            cursor.execute(self.CHECK_QUERY.format(schema, self._TBL_NAME))
            res_gold = cursor.fetchall()
            self._simulate_alter_checkpoint_mode(cluster, cursor)
            cursor.execute("alter table {} alter distkey {}".format(
                self._TBL_NAME, "c_custkey"))
            shadow = self._verify_last_alter_query_bursted(cluster)
            with self.db.cursor() as bootstrap_cursor:
                self._verify_table_owned_by_cluster(
                    bootstrap_cursor, shadow, [('$customer_even', 'Burst')])
            cursor.execute("set query_group to noburst")
            self._simulate_alter_checkpoint_mode(cluster, cursor)
            # Force not to burst
            cursor.execute("alter table {} alter distkey {}".format(
                self._TBL_NAME, "c_custkey"))
            shadow_tbl_id = self._verify_last_alter_query_not_bursted(cluster)
            with self.db.cursor() as bootstrap_cursor:
                self._verify_table_owned_by_cluster(
                    bootstrap_cursor, shadow, [('$customer_even', 'Main')])
                self._start_and_wait_for_refresh(cluster)
                self._verify_table_owned_by_cluster(
                    bootstrap_cursor, shadow, [('$customer_even', 'Main')])
            cursor.execute("set query_group to burst")
            try:
                for i in range(10):
                    cursor.execute("alter table {} alter distkey {}".format(
                        self._TBL_NAME, "c_custkey"))
                    # Verify all alter is not bursted
                    self._verify_last_alter_query_not_bursted_with_shadow(
                        cluster, shadow_tbl_id)
            except NotSupportedError as error:
                assert 'cannot alter to the same distkey' in error.pgerror
                pass
            self._unset_checkpoint_mode(cluster, cursor)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'auto(key(c_custkey))')
            self._verify_table_owned_by_cluster(bootstrap_cursor, shadow, [])

    def test_user_alter_distkey_single_basic(self, cluster):
        """
        The test issue a single user alter distkey on target table and make sure
        at least a copy statement bursted, verify table content and property
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self.insert_and_verify_bursted(cluster, cursor)
            cursor.execute(self.CHECK_QUERY.format(schema, self._TBL_NAME))
            res_gold = cursor.fetchall()
            cursor.execute("set query_group to burst")
            cursor.execute("alter table {} alter distkey {}".format(
                self._TBL_NAME, "c_custkey"))
            self._verify_user_alter_copy_bursted(cluster, schema,
                                                 self._TBL_NAME)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'distkey(c_custkey)')

    def test_user_alter_distkey_multi_in_txn(self, cluster):
        """
        The test issue multiple alter within a txn block on single table
        verified only first alter can be bursted
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self.insert_and_verify_bursted(cluster, cursor)
            cursor.execute(self.CHECK_QUERY.format(schema, self._TBL_NAME))
            res_gold = cursor.fetchall()
            cursor.execute("set query_group to burst")
            cursor.execute("begin")
            cursor.execute("alter table {} alter distkey {}".format(
                self._TBL_NAME, "c_name"))
            shadow_tbl_id = self._verify_user_alter_copy_bursted(
                cluster, schema, self._TBL_NAME)
            # The second alter distkey can not burst due to reason 77
            # concurrency_scaling_status and 7 No backup table accessed.
            # Update : We can now burst no-backup tables. So removing
            # CS 7 from the list of reasons.
            cursor.execute("alter table {} alter distkey {}".format(
                self._TBL_NAME, "c_custkey"))
            self._verify_last_user_alter_copy_not_bursted(
                cluster, shadow_tbl_id, schema, self._TBL_NAME, [(77, )])
            cursor.execute("commit")
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'distkey(c_custkey)')

    @pytest.mark.parametrize("create_checkpoint", [True, False])
    def test_burst_alter_distkey_pre_alter_validation_fails(
            self, cluster, create_checkpoint):
        """
        Validates row count validation failure leads to alter clean up.

        - Optionally alter one iteration to create checkpoint.
        - Inject row count validation failure.
        - Run alter distkey and failed on the injection event.
        - Validate alter distkey clean up.
        - Retry alter until fully converted to distkey.
        - Validate table properties.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        db_session_burst = RedshiftDb(
            get_burst_conn_params(user=LocalhostDefault.BOOTSTRAP_USER))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        fail_validation_event = \
            AlterTableErrorInjectionEvents.EtSimShadowTableRowCountMisMatch
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor, db_session_burst.cursor() \
                as burst_cursor, self.db.cursor() as bootstrap_cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self.insert_and_verify_bursted(cluster, cursor)
            cursor.execute(self.CHECK_QUERY.format(schema, self._TBL_NAME))
            res_gold = cursor.fetchall()
            self._simulate_alter_checkpoint_mode(cluster, cursor)
            if create_checkpoint:
                self.run_alter_and_validate(cluster, cursor, bootstrap_cursor,
                                            burst_cursor)
            # Do fault injection behvaior validation.
            error_message = ''
            try:
                burst_cursor.execute(
                    "xpx 'event set {}'".format(fail_validation_event))
                cursor.execute("alter table {} alter distkey {}".format(
                    self._TBL_NAME, "c_custkey"))
            except InternalError as e:
                error_message = str(e)
            finally:
                burst_cursor.execute(
                    "xpx 'event unset {}'".format(fail_validation_event))
            assert fail_validation_event in error_message, \
                "Expected {}, Got {}".format(fail_validation_event,
                                             error_message)
            # Shadow table on both main and burst are droppped.
            self.check_shadow_table_dropped(burst_cursor, cursor)
            self.check_shadow_table_dropped(bootstrap_cursor, cursor)

            burst_cursor.execute(
                "xpx 'event unset {}'".format(fail_validation_event))
            # Keep altering the table until we bumped into the following error,
            # indicating the table fully converted to distkey.
            for _ in range(2):
                self.run_alter_and_validate(cluster, cursor, bootstrap_cursor,
                                            burst_cursor)
            try:
                for _ in range(8):
                    cursor.execute("alter table {} alter distkey {}".format(
                        self._TBL_NAME, "c_custkey"))
            except NotSupportedError as error:
                assert 'cannot alter to the same distkey' in error.pgerror
            self._unset_checkpoint_mode(cluster, cursor)
            # Validate successful alter distkey.
            self.check_shadow_table_dropped(bootstrap_cursor, cursor)
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'auto(key(c_custkey))')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstAlterDistkeySkew(BaseBurstAlterDistkey):
    CHECK_QUERY = "select count(*), sum(c0), sum(c1) from {}.{}"
    _SCHEMA = ''

    def verify_table_content(self, cursor, res):
        cmd = self.CHECK_QUERY.format(self._SCHEMA, 't1')
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def test_user_alter_distkey_skew(self, cluster):
        """
        The test issue a single user alter distkey on skew target table and make sure
        alter can be bursted correctly
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        with db_session_master.cursor() as cursor:
            cursor.execute("create table t1(c0 int, c1 int) diststyle even")
            cursor.execute("insert into t1 values(1,1)")
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute("insert into t1 values(1,1)")
            self._check_last_query_bursted(cluster, cursor)
            self.CHECK_QUERY = "select count(*), sum(c0), sum(c1) from {}.{}"
            cursor.execute(self.CHECK_QUERY.format(schema, 't1'))
            res_gold = cursor.fetchall()
            cursor.execute("set query_group to burst")
            cursor.execute("alter table t1 alter distkey c1")

        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(
                bootstrap_cursor, schema, 't1', res_gold, 'distkey(c1)')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_ALTER_8_SLICES)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_ALTER_4_SLICES)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstAlterDistkeyMainLessSlices(TestBurstAlterDistkey):
    """
    This test runs basic burst alter test when main cluster has less number
    of data slices, i.e., 4, than the burst cluster, i.e., 8.
    """

    def test_burst_alter_distkey_main_less_slices(self, cluster):
        self.test_burst_alter_distkey_basic(cluster)
