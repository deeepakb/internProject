import logging
import pytest
import getpass
import uuid
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.simulated_helper import encrypted_cluster_fixture, XEN_ROOT_KEY2_HASH
from raff.storage.storage_test import StorageTestSuite
from psycopg2 import InternalError
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite
__all__ = [encrypted_cluster_fixture, super_simulated_mode]
log = logging.getLogger(__name__)
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
MAIN_ROOT_KEY_MISMATCH_ERROR_MSG = \
    ('Main root key sent by query does not match the main root key stored'
     ' on Burst CN node')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.usefixtures("encrypted_cluster_fixture", "super_simulated_mode")
class TestBurstTempTableKeyRotation(BurstTempWrite, StorageTestSuite):
    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_temp_table_key_rotation(self, cluster):
        """
        This test does the following:
        1. encrypt the main cluster through fixture
        2. set up a user temp table
        3. rotate encryption key
        4. burst a temp table query and expect it to fail due to root key mismatch
        5. trigger a refresh which should update the main root key on burst
        6. run _validate_table_content which will run queries both on main and
           burst and compare results are the same
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            self._setup_temp_table(cursor, True, 't1')
            query = "select * from t1;"
            self._validate_table_content(cluster, cursor, query)
            cluster.run_xpx("rotate_key {}".format(XEN_ROOT_KEY2_HASH))
            cursor.execute("set query_group to burst;")
            try:
                cursor.execute(query)
            except InternalError as e:
                assert MAIN_ROOT_KEY_MISMATCH_ERROR_MSG in str(e)
            self._check_last_query_didnt_burst(cluster, cursor)
            # create a perm table in order to trigger a refresh
            self._setup_temp_table(cursor, False, 'p1')
            # refresh should update main root key stored on Burst.
            self._start_and_wait_for_refresh(cluster)
            # _validate_table_content internally executes query
            # on burst and validates query is executed on burst cluster.
            self._validate_table_content(cluster, cursor, query)
