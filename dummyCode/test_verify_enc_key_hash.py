# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import logging
from raff.common.simulated_helper import (XEN_ROOT_KEY1_HASH, XEN_ROOT_KEY2_HASH)
import multiprocessing
from psycopg2 import InternalError
from raff.backup_restore.bar_test import BARTestSuite
from raff.tiered_storage.utils import check_for_process
from raff.util.utils import wait_for

log = logging.getLogger(__name__)


# We store hash values of encryption key in stl_backup_leader table.
# Following tests verify correctness of value stored in the table.
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestEncryptionKeyHash(BARTestSuite):
    def verify_enc_key_hash(self, cursor, key_hash, backup_id):
        # verify  number of columns in stl_backup_leader with enc_key_hash
        cursor.execute("select count(*) from stl_backup_leader where "
                       "enc_key_hash_base64='{}' and backup_id='{}'"
                       .format(key_hash, backup_id))
        assert cursor.fetch_scalar() == 2

    def create_and_load_basic_table(self, cursor):
        cursor.execute("drop table if exists bar_test1;")
        cursor.execute("create table bar_test1 (a bigint, b bigint);")
        cursor.execute("insert into bar_test1 values (1, 1), (1, 2), (1, 3);")

    def populate_bar_test_table(self, cursor, num_records):
        cursor.execute("drop table if exists bar_tests;")
        cursor.execute("create table bar_tests(a float, b float, c float, d float);")
        cursor.execute("insert into bar_tests values "
                       "(random(), random(), random(), random());")

        table_size = 1
        while table_size < num_records:
            cursor.execute("insert into bar_tests "
                           "select random(), random(), random(), random() "
                           "from bar_tests;")
            table_size *= 2

    def cleanup_test_tables(self, cursor):
        cursor.execute("drop table if exists bar_test1;")
        cursor.execute("drop table if exists bar_tests;")

    def simulate_crash(self, cluster, cursor, node_to_crash):
        try:
            xpx_cmd = ("event set EtSimFailCommitP1,"
                       "node={}").format(node_to_crash)
            cluster.run_xpx(xpx_cmd)
            cursor.execute("insert into bar_tests select * from bar_tests limit 100;")
            assert False, 'Expected crash of fdisk.'
        except InternalError as e:
            log.info("Output of exception is is {}".format(e))

    def crash_node_and_reboot(self, cluster, cursor, node_to_crash):
        def is_diskman_dead(node_num):
            return not check_for_process("diskman{}".format(node_num))

        async_thread = multiprocessing.Process(
            target=self.simulate_crash,
            args=[cluster, cursor, node_to_crash])
        async_thread.start()
        # Sleep for 60 seconds to ensure completion before crash
        # is simulated.
        async_thread.join(timeout=60)

        # Check diskman on 'node_num_to_crash' is actually dead.
        # It takes sometime for probe file to be updated, hence the
        # retries.
        wait_for(func=is_diskman_dead, args=[node_to_crash],
                 retries=20, delay_seconds=10)
        cluster.reboot_cluster()
        xpx_cmd = ("event unset EtSimFailCommitP1," "node={}").format(node_to_crash)
        cluster.run_xpx(xpx_cmd)

    # Test correctness of hash value of encryption key after backup
    def test_encryption_key_hash_after_backup(self, cluster_session):
        with cluster_session(
                enable_encryption=True,
                clean_db_before=True,
                clean_db_after=True):
            with self.db.cursor() as cursor:
                # create tables and add data
                self.create_and_load_basic_table(cursor)
                backup_id = self.take_backup()

                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

                # add more data
                self.populate_bar_test_table(cursor, 100)
                backup_id = self.take_backup()
                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

    # Test correctness of hash value of encryption key after commit failure
    # TODO(Redshift-5736): Remove no_jdbc when the related issue is resolved.
    @pytest.mark.no_jdbc
    def test_encryption_key_hash_on_commit_failure(self, cluster_session, cluster):
        with cluster_session(
                enable_encryption=True,
                clean_db_before=True,
                clean_db_after=True):
            row_count = 0
            with self.db.cursor() as cursor:
                # add data
                self.populate_bar_test_table(cursor, 100)
                # Note initial amount of data
                cursor.execute("select count(*) from bar_tests;")
                row_count = cursor.fetch_scalar()

                # Crash node 0
                node_to_crash = 0
                self.crash_node_and_reboot(cluster, cursor, node_to_crash)
            with self.db.cursor() as cursor:
                backup_id = self.take_backup()
                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

                # Crash node 1
                node_to_crash = 1
                self.crash_node_and_reboot(cluster, cursor, node_to_crash)
            with self.db.cursor() as cursor:
                # Verify no new data was inserted during these crashes
                cursor.execute("select count(*) from bar_tests")
                assert cursor.fetch_scalar() == row_count

                backup_id = self.take_backup()
                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

                # add more data
                self.populate_bar_test_table(cursor, 100)
                backup_id = self.take_backup()
                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

    # Test correctness of hash value of encryption key after key rotation
    def test_encryption_key_hash_after_key_rotation(self, cluster_session, cluster):
        with cluster_session(
                enable_encryption=True,
                clean_db_before=True,
                clean_db_after=True):
            with self.db.cursor() as cursor:
                # create tables and add data
                self.create_and_load_basic_table(cursor)
                backup_id = self.take_backup()

                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY1_HASH, backup_id)

                cluster.run_xpx('rotate_key {}'.format(XEN_ROOT_KEY2_HASH))

                backup_id = self.take_backup()

                self.verify_enc_key_hash(cursor, XEN_ROOT_KEY2_HASH, backup_id)
