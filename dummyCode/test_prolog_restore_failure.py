# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import random
import signal
import pytest
import uuid

from raff.common import TableNotRestoredError
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.simulated_helper import (
    LocalhostRestoreError, SimDbConnectError, copy_snapshot_post_backup,
    is_rerepp_complete, restore_from_localhost_snapshot)
from raff.s3commit.db_helpers import load_single_table
from raff.storage.storage_test import StorageTestSuite
from raff.util.utils import wait_for
from raff.xrestore.xrestore_test import XRestoreTestSuite

log = logging.getLogger(__name__)

XRESTORE_TIMEOUT_SECONDS = 300


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestPrologRestoreFailure(XRestoreTestSuite, StorageTestSuite):

    def _backup_and_restore(self, cluster, db_session, perform_restore_func,
                            original_table, restored_table, fatal, error_type,
                            is_single_node_failure):
        """
        Creates a dummy table, and then performs a restore with an injected
        error. If the error is fatal, verify that the restore failed. If the
        error is non-fatal, verify that the restore successfully completed.

        Args:
            cluster (LocalhostCluster): A localhost cluster object.
            db_session (DbSession): A database session object.
            perform_restore_func (function): Performs a restore on a backup ID.
            original_table (str): The name of the table to be backed up.
            restored_table (str): The name of the restored table.
            fatal (bool): Whether the prolog restore failure should be fatal.
            error_type (type): The exception type thrown if the restore fails.
            is_single_node_failure (bool): Whether to fail only one CN.

        Raises:
            AssertionError: If the restored data is incorrect, or if the restore
                unexpectedly succeeded or failed.
        """
        # Create a table and insert some dummy data
        backup_content = load_single_table(db_session, original_table).sort()

        # Perform a backup
        backup_id = str(uuid.uuid4())
        cluster.backup_cluster(backup_id)
        copy_snapshot_post_backup(backup_id)

        # Set the prolog restore failure event
        events = 'fatal={}'.format(int(fatal))
        if is_single_node_failure:
            node = random.randrange(cluster.node_count)
            events += ',node={}'.format(node)
            log.info("Injecting failure into only CN{}".format(node))

        with self.run_with_event_set_in_padb(
                cluster, ['EtPrologRestoreFailure,' + events]):
            cluster.reboot_cluster()

            restore_failed = False
            try:
                # Perform the restore
                perform_restore_func(backup_id)
            except (error_type, SimDbConnectError):
                # Initdb if the restore failed
                restore_failed = True
                cluster.initdb()
            else:
                # Otherwise, verify that the restore succeeded
                with db_session.cursor() as cursor:
                    cursor.execute("select * from {}".format(restored_table))
                    restored_content = cursor.result_fetchall().rows.sort()
                    assert backup_content == restored_content, \
                        "Restore finished but validation failed due to unique " \
                        "content in backup: {} and restored: {}".format(
                            set(backup_content) - set(restored_content),
                            set(restored_content) - set(backup_content))
            finally:
                # Make sure the restore succeeded or failed as expected
                assert restore_failed == fatal, \
                    "Expected prolog restore failure: {}. Actual: {}".format(
                        fatal, restore_failed)

    def _perform_classic_restore(self, cluster, backup_id):
        """
        Perform a classic restore.

        Args:
            cluster (LocalhostCluster): A localhost cluster object.
            backup_id (str): The snapshot ID to restore from.
        """
        log.info("Classic restore from snapshot {}".format(backup_id))
        restore_from_localhost_snapshot(backup_id)
        wait_for(is_rerepp_complete, retries=3, delay_seconds=10)
        cluster.run_xpx('restore_bootstrap_mode unset')
        log.info("Classic restore finished from {}".format(backup_id))

    def _perform_xrestore(self, cluster, db_session, backup_id, gucs,
                          cluster_size):
        """
        Perform an XRestore.

        Args:
            cluster (LocalhostCluster): A localhost cluster object.
            db_session (DbSession): A database session object.
            backup_id (str): The snapshot ID to restore from.
            gucs (dict): The GUCs to set on the restored cluster.
            cluster_size (int): The number of nodes in the restored cluster.
        """
        # Begin XRestore and start SIGALRM timer
        log.info("XRestore from snapshot {}".format(backup_id))
        signal.alarm(XRESTORE_TIMEOUT_SECONDS)
        self.do_xrestore(
            db_session, cluster, backup_id, cluster_size, extra_gucs=gucs)

        # Cancel the SIGALRM timer
        signal.alarm(0)
        log.info("XRestore finished from {}".format(backup_id))

    def _perform_tlr(self, cluster, db_session, backup_id, source_table,
                     target_table):
        """
        Perform a table-level restore.

        Args:
            cluster (LocalhostCluster): A localhost cluster object.
            db_session (DbSession): A database session object.
            backup_id (str): The snapshot ID to restore from.
            source_table (str): The name of the source table.
            target_table (str): The name of the target table.
        """
        log.info("TLR from snapshot {} for table {}".format(
            backup_id, source_table))
        cluster.restore_table(
            snapshot_identifier=backup_id,
            source_database_name=db_session.dbname,
            source_table_name=source_table,
            new_table_name=target_table,
            wait_table_hydrated=True)
        log.info("TLR finished from {}".format(backup_id))

    @pytest.mark.parametrize(
        "fatal, is_single_node_failure",
        [
            (True, False),      # Fatal error on all nodes
            (False, False),     # Non-fatal error on all nodes
            (False, True)       # Non-fatal error on single node
        ])
    def test_prolog_classic_restore_failure(self, cluster, db_session, fatal,
                                            is_single_node_failure):
        """
        This test verifies that classic restore successfully falls back to
        segmented restore after a non-fatal failure, and fails after a fatal
        failure. Non-fatal failures have the option to be injected into either
        one or all of the CNs.
        """
        cluster_session = ClusterSession(cluster)
        gucs = {
            "restore_from_backup_sb_prolog": "true",
            "map_xinvariant_to_xcheck": "false"
        }
        table_name = "test_classic_restore_table"

        with cluster_session(
                gucs=gucs, clean_db_before=True, clean_db_after=True):
            # Perform the restore and make sure it succeeded/failed as expected
            self._backup_and_restore(
                cluster, db_session,
                lambda backup_id: self._perform_classic_restore(
                    cluster, backup_id),
                table_name, table_name, fatal, LocalhostRestoreError,
                is_single_node_failure)

    @pytest.mark.parametrize(
        "fatal, is_single_node_failure",
        [
            (True, False),      # Fatal error on all nodes
            (False, False),     # Non-fatal error on all nodes
            (False, True)       # Non-fatal error on single node
        ])
    def test_prolog_xrestore_failure(self, cluster, db_session, fatal,
                                     is_single_node_failure):
        """
        This test verifies that XRestore successfully falls back to segmented
        restore after a non-fatal failure, and fails after a fatal failure.
        Non-fatal failures have the option to be injected into either one or
        all of the CNs.
        """
        cluster_session = ClusterSession(cluster)
        source_cluster_size = cluster.node_count
        target_cluster_size = source_cluster_size + 1
        gucs = {
            "xrestore_from_backup_sb_prolog": "true",
            "map_xinvariant_to_xcheck": "false"
        }
        table_name = "test_xrestore_table"

        def handler(*_):
            """
            XRestore's built-in timeout is too long, so we use a signal handler
            to timeout if it doesn't succeed within our specified time limit.
            """
            raise TimeoutError()

        with cluster_session(clean_db_before=True, clean_db_after=True):
            # Set up the cluster
            self.do_xrestore_setup(
                cluster, source_cluster_size, extra_gucs=gucs)
            signal.signal(signal.SIGALRM, handler)

            # Perform the restore and make sure it succeeded/failed as expected
            self._backup_and_restore(
                cluster, db_session,
                lambda backup_id: self._perform_xrestore(
                    cluster, db_session, backup_id, gucs, target_cluster_size),
                table_name, table_name, fatal, TimeoutError,
                is_single_node_failure)

            # Reset the signal handler
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @pytest.mark.parametrize(
        "fatal, is_single_node_failure",
        [
            (True, False),      # Fatal error on all nodes
            (False, False),     # Non-fatal error on all nodes
            (False, True)       # Non-fatal error on single node
        ])
    def test_prolog_tlr_failure(self, cluster, fatal, is_single_node_failure):
        """
        This test verifies that TLR successfully falls back to segmented restore
        after a non-fatal failure, and fails after a fatal failure. Non-fatal
        failures have the option to be injected into either one or all of the
        CNs.
        """
        cluster_session = ClusterSession(cluster)
        ctx = SessionContext(schema="public")
        gucs = {
            "tlr_from_backup_sb_prolog": "true",
            "map_xinvariant_to_xcheck": "false"
        }
        source_table = "test_tlr_table"
        target_table = "{}_restored".format(source_table)

        with cluster_session(gucs=gucs, clean_db_before=True,
                             clean_db_after=True), \
                DbSession(cluster.get_conn_params(),
                          session_ctx=ctx) as db_session:
            # Perform the restore and make sure it succeeded/failed as expected
            self._backup_and_restore(
                cluster, db_session,
                lambda backup_id: self._perform_tlr(
                    cluster, db_session, backup_id, source_table, target_table),
                source_table, target_table, fatal, TableNotRestoredError,
                is_single_node_failure)
