import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode, \
    get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite
from raff.common.dimensions import Dimensions
from raff.burst.burst_unload.burst_unload_test_helper import BurstTestUtils

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
INSERT_STMT = "INSERT INTO {schema}.{table} SELECT * FROM {schema}.{table} LIMIT 4;"


@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ('enable_burst_failure_handling', 'true'),
        ('burst_enable_write_user_temp_ctas', 'false')]))
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ('enable_burst_failure_handling', 'true'),
        ('burst_enable_write_user_temp_ctas', 'false')]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempErrorHandling(BurstTempWrite):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                event=['EtFakeBurstErrorDispatchTTHeadersToCN',
                       'EtFakeBurstErrorAfterStreamHeader',
                       'EtFakeBurstErrorAfterSnapIn1',
                       'EtFakeBurstErrorAfterSnapIn2']))

    def verify_burst_query_status(self, cluster, dml, status):
        query = """
            select query from stl_query
            where userid > 1
            and querytxt ilike '%{}%'
            order by starttime desc
            limit 1
            """.format(dml)
        with self.db.cursor() as cur:
            cur.execute(query)
            qid = cur.fetch_scalar()
            self.verify_query_status(cluster, qid, status)

    def test_burst_temp_error_retry(self, cluster, vector):
        """
        Test temp table error handling
        1. Introduce error at different places on burst side
        2. Validate temp table write query gets executed
           on main or gets aborted incase of error on burst.
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with db_session.cursor() as dml_cursor, \
             self.db.cursor() as bootstrap_cursor, \
             burst_db.cursor() as burst_cursor:
            btu = BurstTestUtils(self.db, cluster, db_session)
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, dml_cursor,
                                                   max_tables_to_load=1)
            temp_table_name = list(temp_table_dict.keys())[0]
            temp_table_schema = list(temp_table_dict.values())[0]
            if 'AfterSnapIn' in vector.event:
                bootstrap_cursor.execute("xpx 'event set {}'".format(vector.event))
            else:
                burst_cursor.execute("xpx 'event set {}'".format(vector.event))
            status = 0 if ('DispatchTTHeadersToCN' in vector.event) else 27
            dml_cmd = INSERT_STMT.format(schema=temp_table_schema,
                                         table=temp_table_name)
            # For event EtFakeBurstErrorDispatchTTHeadersToCN, query reruns on main
            # if failure handling is enabled
            if 'DispatchTTHeadersToCN' in vector.event:
                dml_cursor.execute(dml_cmd)
            # For other events, query gets aborted on burst
            else:
                error = "Test error" if 'AfterSnapIn' in vector.event \
                    else "error after streaming block header"
                btu.execute_failing_query(dml_cursor, dml_cmd, error)
            self.verify_burst_query_status(cluster, dml_cmd, status)
            if 'AfterSnapIn' in vector.event:
                bootstrap_cursor.execute("xpx 'event unset {}'".format(vector.event))
            else:
                burst_cursor.execute("xpx 'event unset {}'".format(vector.event))
