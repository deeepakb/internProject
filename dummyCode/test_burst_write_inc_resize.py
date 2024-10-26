import pytest
import logging

from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode_method)
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.db.session_context import SessionContext
from raff.common.simulated_helper import _get_padb_conf_value
from raff.incremental_resize.incremental_resize_test import (
    IncResizeTestSuite)
from test_burst_write_equivalence import TestBurstWriteBlockEquivalenceBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode_method]


@pytest.mark.encrypted_only
@pytest.mark.no_jdbc
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ("burst_enforce_user_temp_table_complete_rerep_to_s3", 'false'),
        ("disk_mirror_count", '1'),
        ("force_upgrade_systbl", 'false'),
        ("incresize_randomize_slice_placement", 'false'),
        ("redshift_row_fetcher_enable_workstealing", 'false'),
        ("superblock_vacuum_enabled", '0'),
        ("vacuum_auto_worker_enable", 'false'),
        ("var_slice_mapping_policy", '0')]))
class TestBurstWriteIncResize(IncResizeTestSuite,
                              TestBurstWriteBlockEquivalenceBase):
    @classmethod
    def modify_test_dimensions(cls):
        inc_resize_combination = [(3, 4), (3, 2)]
        return Dimensions(
            dict(
                up_down=inc_resize_combination,
                sortkey=['sortkey(c1, c0)'],
                size=['large'],
                diststyle=['diststyle even', 'distkey(c0)']))

    @pytest.mark.session_ctx(user_type='super')
    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_write_inc_resize(self, cluster, db_session, vector):
        '''
        In this test we create a cluster, load perm tables and validate
        queries on these perm tables execute on burst cluster. Subsequently,
        we create 4 active sessions and load temp tables in those sessions.
        The test then performs elastic resize scale-up/down, validates
        active sessions with temp tables are preserved across the resize
        operation and validates table content of perm and temp tables.
        '''
        test_schema = 'public'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "burst_write_inc_resize"
        try:
            # Disable re-replication to enforce scan from previous guid for
            # temp tables that were re-guid'ed during elastic resize.
            cluster.set_events_in_padb_conf(['EtDisableRereplication'])
            with DbSession(
                    cluster.get_conn_params(user='master'),
                    session_ctx=session_ctx1) as session1:
                dml_cursor = session1.cursor()
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                log.info("check metadata before experiments")
                self._insert_tables(cluster, dml_cursor, base_tbl_name)
                self._delete_tables(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1(cluster, dml_cursor, base_tbl_name)
                self._update_tables_2(cluster, dml_cursor, base_tbl_name)
            # Create live sessions with temp tables to validate session
            # persistence as well temp tables persistence after elastic resize.
            num_sessions = 4
            self.include_temp_table = True
            self.create_sessions_load_temp_table(
                cluster, db_session, num_sessions,
                load_compact_temp_tables=True)
            # Perform elastic resize.
            orig_size = vector.up_down[0]
            resize_to_size = vector.up_down[1]
            num_slices =\
                int(_get_padb_conf_value("slices_per_node")) * orig_size
            log.info("Resizing from {} nodes to {} nodes".format(
                orig_size, resize_to_size))
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                self.do_inc_resize(
                    cluster, session2, orig_size, resize_to_size)
            # Validate sessions with temp tables are retained after resize.
            self.validate_session_temp_tables(num_slices)
            # Validate table content for perm tables.
            with DbSession(
                    cluster.get_conn_params(user='master'),
                    session_ctx=session_ctx1) as session1:
                dml_cursor = session1.cursor()
                # Validate the table content.
                self._validate_content_equivalence(dml_cursor, base_tbl_name)
                self._validate_table(cluster, test_schema,
                                     base_tbl_name + "_burst", vector.diststyle)
                self._validate_table(cluster, test_schema,
                                     base_tbl_name + "_not_burst",
                                     vector.diststyle)
                # Drop the tables at end of the test.
                self._drop_tables(session1, base_tbl_name)
        finally:
            cluster.unset_event('EtDisableRereplication')
