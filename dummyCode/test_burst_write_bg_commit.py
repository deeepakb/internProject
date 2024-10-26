# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.simulated_helper import create_localhost_snapshot
from raff.burst.burst_write import BurstWriteTest
from test_burst_write_equivalence import TestBurstWriteBlockEquivalenceBase
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {};"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10);"

S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")


class BurstWriteBGCommitBase(TestBurstWriteBlockEquivalenceBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even'],
                sortkey=['']))

    def _setup_table(self, cursor, tbl_name, vector):
        cursor.execute("DROP TABLE IF EXISTS {};".format(tbl_name))
        cursor.execute(
            CREATE_STMT.format(tbl_name, vector.diststyle,
                                vector.sortkey))
        cursor.execute("GRANT ALL ON TABLE {} TO PUBLIC;".format(tbl_name))

    def verify_table_content(self, cursor, tbl, res):
        cmd = "select count(*), sum(c0), sum(c1) from {};".format(tbl)
        cursor.execute(cmd)
        real_res = cursor.fetchall()
        log.info("verify_table_content, real res: {}".format(real_res))
        assert real_res == res

    def _insert_copy_table(self, cluster, cursor, tbl_name, burst):
        if burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        # insert before copy in case compupdate.
        cursor.execute(INSERT_CMD.format(tbl_name))
        cursor.execute(COPY_STMT.format(tbl_name, S3_PATH))
        if burst:
            self._check_last_copy_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)

    def _has_any_persistent_hdr(self, cursor, tbl_name,
            initial_num_visible_cols = 0, num_new_cols = 100000):
        """
        Returns True if there is any persistent hdr in the table in
        the given column range.
        Default values of initial_num_visible_cols and num_new_cols are set
        to 0 and 100000, such that when nothing is passed, all columns of
        the table would be considered.
        """
        cursor.execute("set query_group to metrics;")
        cursor.execute("""SELECT COUNT(*) FROM stv_blocklist WHERE tbl =
                          (SELECT MAX(oid) FROM pg_class WHERE relname = '{}')
                          AND (col BETWEEN {} AND {}) AND (flags & (1<<17) <> 0)
                          """.format(tbl_name, initial_num_visible_cols,
                              initial_num_visible_cols + num_new_cols - 1))
        num_persistent_hdr = cursor.fetch_scalar()
        return num_persistent_hdr != 0

    def _all_hdrs_persistent(self, cursor, tbl_name):
        """
        Returns true if all hdrs of the table are marked as persistent.
        """
        cursor.execute("""SELECT COUNT(*) FROM stv_blocklist WHERE tbl =
                          (SELECT MAX(oid) FROM pg_class WHERE relname = '{}')
                          AND flags & (1<<17) = 0
                          """.format(tbl_name))
        num_non_persistent_hdr = cursor.fetch_scalar()
        return num_non_persistent_hdr == 0

    def _get_tx_id(self, cursor):
        cursor.execute("SELECT txid_current()")
        return cursor.fetch_scalar()

    def _is_xid_committed(self, cursor, xid):
        log.info("_is_xid_committed, xid: {}".format(xid))
        cursor.execute("SELECT COUNT(*) FROM stl_commit_stats WHERE xid = {}".
                format(xid))
        return cursor.fetch_scalar() != 0

    def _validate_blocks_flags(self, cluster, cursor, base_tbl_name):
        # Not considering rowid column because rowid can be different when
        # inserted slices are different.
        # For flags, only BLOCK_CKSUMABLE_FLAGS
        # (except BLOCK_HAS_PREFERRED_DISK) are considered.
        stv_cmd = ("select distinct(flags & (33529898 - (1<<15)))"
                   " from stv_blocklist where tbl = '{}'::regclass::oid")
        except_cmd = "{} except {};"
        nonburst_tbl_stv_cmd = stv_cmd.format(base_tbl_name + "_not_burst")
        burst_tbl_stv_cmd = stv_cmd.format(base_tbl_name + "_burst")
        check_cmd_1 = except_cmd.format(nonburst_tbl_stv_cmd, burst_tbl_stv_cmd)
        check_cmd_2 = except_cmd.format(burst_tbl_stv_cmd, nonburst_tbl_stv_cmd)
        cursor.execute("set query_group to metrics;")
        cursor.execute(nonburst_tbl_stv_cmd)
        res_1 = cursor.fetchall()
        cursor.execute(burst_tbl_stv_cmd)
        res_2 = cursor.fetchall()
        log.info("_validate_blocks_flags nonburst tbl= {}".format(res_1))
        log.info("_validate_blocks_flags burst tbl= {}".format(res_2))

        cursor.execute(check_cmd_1)
        res_1 = cursor.fetchall()
        cursor.execute(check_cmd_2)
        res_2 = cursor.fetchall()
        assert res_1 == [] and res_1 == res_2
        log.info("_validate_blocks_flags except1= {}".format(res_1))
        log.info("_validate_blocks_flags except2= {}".format(res_2))

    def _acquire_burst_via_xpx(self, cluster):
        snapshot_id = "burst-write-snapshot-{}".format(str(uuid.uuid4().hex))
        if cluster.host_type == HostType.CLUSTER:
            cluster.backup_cluster(snapshot_id)
        else:
            create_localhost_snapshot(snapshot_id, wait=True)
        _, stdout, stderr = cluster.run_xpx('burst_acquire')
        arn = stderr[stderr.find("arn:aws:redshift"):].rstrip()
        if not arn:
            raise FailedToAcquireBurstCluster(
                ("The xpx command did not return an arn.  stdout: {}\n"
                 "stderr: {}").format(stdout, stderr))
        return arn

    def do_test_burst_write_bg_commit(self, cluster, vector):
        """
        Run the same insert, delete, update, copy workload on a burst and
        a non-burst table. Trigger background commit on main cluster, and
        validate the superblock commit is correct through checking block header
        flags.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        session_ctx3 = SessionContext(user_type='bootstrap')
        base_tbl_name = "burst3419"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1, \
                session1.cursor() as burst_cursor, \
                DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2,\
                session2.cursor() as nonburst_cursor,\
                DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx3) as session3,\
                session3.cursor() as check_cursor:
            burst_cursor.execute(
                "set search_path to {}".format(test_schema))
            nonburst_cursor.execute(
                "set search_path to {}".format(test_schema))
            check_cursor.execute(
                "set search_path to {}".format(test_schema))
            self._setup_table(
                nonburst_cursor, base_tbl_name + "_not_burst", vector)
            self._setup_table(burst_cursor, base_tbl_name + "_burst", vector)
            self._start_and_wait_for_refresh(cluster)

            nonburst_cursor.execute('begin;')
            burst_cursor.execute('begin;')
            self._insert_copy_table(cluster, nonburst_cursor,
                             base_tbl_name + "_not_burst", False)
            self._insert_copy_table(cluster, burst_cursor,
                             base_tbl_name + "_burst", True)
            # Running a commit in another session should not mark the
            # uncommitted transaction in the first session as committed.
            tx_id_nonburst = self._get_tx_id(nonburst_cursor)
            tx_id_burst = self._get_tx_id(burst_cursor)
            cluster.run_xpx('hello')
            assert not self._is_xid_committed(check_cursor, tx_id_nonburst)
            assert not self._is_xid_committed(check_cursor, tx_id_burst)
            # Data blocks should be persistent because table is not created in
            # the transaction.
            assert self._has_any_persistent_hdr(
                check_cursor, base_tbl_name + "_not_burst")
            assert self._has_any_persistent_hdr(
                check_cursor, base_tbl_name + "_burst")
            self.verify_table_content(
                check_cursor, base_tbl_name + "_not_burst", [(0, None, None)])
            self.verify_table_content(check_cursor, base_tbl_name + "_burst",
                                      [(0, None, None)])
            nonburst_cursor.execute('commit;')
            burst_cursor.execute('commit;')
            # Now the transaction in our session should be marked as committed.
            assert self._is_xid_committed(check_cursor, tx_id_nonburst)
            assert self._is_xid_committed(check_cursor, tx_id_burst)
            assert self._all_hdrs_persistent(check_cursor,
                                             base_tbl_name + "_not_burst")
            assert self._all_hdrs_persistent(check_cursor,
                                             base_tbl_name + "_burst")
            # Validate the table content against the identical not
            # bursted case and also validates between the burst cluster
            # the main cluster.
            self._validate_blocks_flags(cluster, check_cursor, base_tbl_name)
            self._validate_content_equivalence(nonburst_cursor, base_tbl_name)
            nonburst_cursor.execute("grant all on {} to public;".format(
                base_tbl_name + "_not_burst"))
            self._insert_tables(cluster, burst_cursor, base_tbl_name)
            self._delete_tables(cluster, burst_cursor, base_tbl_name)
            self._update_tables_1(cluster, burst_cursor, base_tbl_name)
            assert self._all_hdrs_persistent(check_cursor,
                                             base_tbl_name + "_burst")
            assert self._all_hdrs_persistent(check_cursor,
                                             base_tbl_name + "_not_burst")
            self._validate_table(cluster, test_schema,
                                 base_tbl_name + "_burst", vector.diststyle)
            self._validate_table(cluster, test_schema,
                                 base_tbl_name + "_not_burst",
                                 vector.diststyle)

    def do_test_burst_write_bg_commit_restart(self, db_session, cluster,
                                              vector):
        """
        create table tbl_not_burst, tbl_burst.

        nonburst_cursor            burst_cursor
        begin;
                                   begin;
        insert tbl_not_burst;
        copy tbl_not_burst;
                                   insert tbl_burst;
                                   copy tbl_burst;
                                                        cluster.run_xpx('hello')
                                                        cluster.reboot_cluster()
        after reboot is done, check the content of the table. both should be empty.
        """

        db_session_master1 = DbSession(cluster.get_conn_params(user='master'))
        db_session_master2 = DbSession(cluster.get_conn_params(user='master'))
        db_session_master3 = DbSession(cluster.get_conn_params())
        base_tbl_name = "burst3419"
        with db_session_master1.cursor() as burst_cursor, \
                db_session_master2.cursor() as nonburst_cursor, \
                self.db.cursor() as check_cursor:
            burst_cursor.execute("set search_path to public;")
            nonburst_cursor.execute("set search_path to public;")
            # check_cursor.execute("set session authorization default")
            self._setup_table(nonburst_cursor, base_tbl_name + "_not_burst",
                              vector)
            self._setup_table(burst_cursor, base_tbl_name + "_burst", vector)
            self._start_and_wait_for_refresh(cluster)
            nonburst_cursor.execute('begin;')
            burst_cursor.execute('begin;')
            self._insert_copy_table(cluster, nonburst_cursor,
                                    base_tbl_name + "_not_burst", False)
            self._insert_copy_table(cluster, burst_cursor,
                                    base_tbl_name + "_burst", True)
            # Running a commit in another session should not mark the
            # uncommitted transaction in the first session as committed.
            global tx_id_nonburst
            tx_id_nonburst = self._get_tx_id(nonburst_cursor)
            global tx_id_burst
            tx_id_burst = self._get_tx_id(burst_cursor)
            log.info("tx_id_nonburst: {}, tx_id_burst: {}".format(
                tx_id_nonburst, tx_id_burst))
            cluster.run_xpx('hello')
            assert not self._is_xid_committed(check_cursor, tx_id_nonburst)
            assert not self._is_xid_committed(check_cursor, tx_id_burst)
            # Data blocks should be persistent because table is not created in
            # the transaction.
            assert self._has_any_persistent_hdr(check_cursor,
                                                base_tbl_name + "_not_burst")
            assert self._has_any_persistent_hdr(check_cursor,
                                                base_tbl_name + "_burst")
            # Trigger cluster restart after bg commit.
            cluster.reboot_cluster()
        cluster.wait_for_cluster_available(180)

        log.info("after reboot, tx_id_nonburst: {}, tx_id_burst: {}".format(
            tx_id_nonburst, tx_id_burst))

        with db_session_master1.cursor() as burst_cursor, \
                db_session_master2.cursor() as nonburst_cursor, \
                self.db.cursor() as check_cursor:
            burst_cursor.execute("set search_path to public;")
            nonburst_cursor.execute("set search_path to public;")
            assert not self._is_xid_committed(check_cursor, tx_id_nonburst)
            assert not self._is_xid_committed(check_cursor, tx_id_burst)
            log.info("time to check table content")
            self.verify_table_content(
                check_cursor, base_tbl_name + "_not_burst", [(0, None, None)])
            self.verify_table_content(check_cursor, base_tbl_name + "_burst",
                                      [(0, None, None)])

            # Validate the table content against the identical not
            # bursted case and also validates between the burst cluster
            # the main cluster.
            self._validate_blocks_flags(cluster, check_cursor, base_tbl_name)
            self._validate_content_equivalence(nonburst_cursor, base_tbl_name)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'commit_superblock_sweep_mode': '0',
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteBGCommit0(BurstWriteBGCommitBase):
    def test_burst_write_bg_commit_0(self, cluster, vector):
        self.do_test_burst_write_bg_commit(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.custom_local_gucs(gucs={
    'commit_superblock_sweep_mode': '0',
})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.encrypted_only
class TestBurstWriteBGCommitRestart0(BurstWriteBGCommitBase):
    def test_burst_write_bg_commit_restart_0(self, db_session, cluster,
                                             vector):
        self.do_test_burst_write_bg_commit_restart(db_session, cluster, vector)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'commit_superblock_sweep_mode': '1',
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteBGCommit1(BurstWriteBGCommitBase):
    def test_burst_write_bg_commit_1(self, cluster, vector):
        self.do_test_burst_write_bg_commit(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.custom_local_gucs(gucs={
    'commit_superblock_sweep_mode': '1',
})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.encrypted_only
class TestBurstWriteBGCommitRestart1(BurstWriteBGCommitBase):
    def test_burst_write_bg_commit_restart_1(self, db_session, cluster,
                                             vector):
        self.do_test_burst_write_bg_commit_restart(db_session, cluster, vector)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true',
})
# Setting superblock sweep optimization in dual-mode.
@pytest.mark.custom_burst_gucs(gucs={
    'commit_superblock_sweep_mode': '1',
    'map_xinvariant_to_xcheck': 'false',
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteSBSweepOptInvariantFailure(BurstWriteTest):
    def _num_sb_sweep_invariant_logged(self, cursor):
        cursor.execute("""SELECT COUNT(*) FROM stl_invariant
                          WHERE name = 'CommitUnexpectedModifiedHdrs' """)
        return cursor.fetch_scalar()

    def _setup_table(self, cursor):
        cursor.execute("""DROP TABLE IF EXISTS burst3419""")
        cursor.execute("""CREATE TABLE burst3419(c0 int, c1 int)
                          diststyle even;""")
        cursor.execute("""INSERT INTO burst3419 values(1,2);""")
        for i in range(5):
            cursor.execute("""INSERT INTO burst3419 SELECT * FROM burst3419;""")

    def _run_table_create_and_insert(self, cluster, cursor):
        cursor.execute("""DROP TABLE IF EXISTS burst3419_copy""")
        cursor.execute("""CREATE TABLE burst3419_copy AS
                          select * from burst3419;""")
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        cursor.execute("""INSERT INTO burst3419 select * from burst3419;""")
        self._check_last_query_bursted(cluster, cursor)

    def test_burst_write_sb_sweep_opt_invariant_failure(self, cluster):
        """
        Superblock sweep optimization is not working for burst write currently.
        So the invariant should not be triggered no matter event
        EtSkipHdrTrackingAtSnap is set or not.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        conn_params = cluster.get_conn_params(user='master')
        with DbSession(conn_params, session_ctx=session_ctx1) as db_session,\
                db_session.cursor() as cursor, \
                DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as db_session_bs,\
                db_session_bs.cursor() as cursor_bs:
            self._setup_table(cursor)
            with cluster.event('EtSkipHdrTrackingAtSnap'):
                num_sb_sweep_invariant_logged_before = \
                    self._num_sb_sweep_invariant_logged(cursor_bs)
                self._run_table_create_and_insert(cluster, cursor)
                assert (self._num_sb_sweep_invariant_logged(cursor_bs) ==
                        num_sb_sweep_invariant_logged_before)
            # Now test the similar commands without EtSkipHdrTrackingAtSnap
            # event being set. There shouldn't be any invariant failure.
            num_sb_sweep_invariant_logged_before = \
                self._num_sb_sweep_invariant_logged(cursor_bs)
            self._run_table_create_and_insert(cluster, cursor)
            assert (self._num_sb_sweep_invariant_logged(cursor_bs) ==
                    num_sb_sweep_invariant_logged_before)
