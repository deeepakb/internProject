import logging
import pytest

from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.precommit
class TestBurstTableDefinition(MonitoringTestSuite):
    """ Check the generation and rotation of subscribed log."""

    # Returns list of stcs table name created in database.
    stcs_tables_query = ("SELECT DISTINCT tablename "
                         "FROM pg_catalog.pg_tables "
                         "WHERE schemaname = 'pg_catalog' "
                         "AND tablename LIKE 'stcs_%';")

    def get_stcs_table_names(self, cluster):
        """ Get list of stcs table name created in database."""

        conn_params = cluster.get_conn_params()
        ctx = SessionContext(user_type=SessionContext.BOOTSTRAP)
        err_msg = "No stcs system table found from database."
        with DbSession(conn_params, session_ctx=ctx) as db_session:
            with db_session.cursor() as cursor:
                cursor.execute(self.stcs_tables_query)
                stcs_list_result = cursor.fetchall()
                stl_table_count = len(stcs_list_result)
                assert (stl_table_count > 0), err_msg
                log.info('{} stcs system tables are found:\n'.format(
                    stl_table_count))
                log.info(stcs_list_result)
                return stcs_list_result

    def test_stcs_definition(self, cluster):
        """
        For a given stcs table, it's log content is subscribed and forked
        from original STLL table logging. This means for each stcs table:
            1) There will always be a corresponding STLL exists
            2) The stcs table definition should be 'identical' as the STLL
            in terms of number of columns, column types, and column order.
            This is true except:
                - When the column type is timestamp (time) in STLL table,
                the column type in STLL table will be bigint (long). This
                difference is created because query of STL table supports
                storing timestamp as int64 in text format but Spectrum
                does not. So we will need to do view level conversion of
                this bigint to timestamp as a human readable format.
                - stcs table during query processing, will be changed to
                a Spectrum table and there're two pseudo columns (__path
                and __size) automatically get added as the last two
                columns of stcs, this means st table will have two extra
                columns than STLL when query.

        This test verifies to ensure all above facts are true.
        """

        db_session = DbSession(
            cluster.get_conn_params(db_name="dev"),
            session_ctx=SessionContext(user_type=SessionContext.BOOTSTRAP))
        """
        Return result for stcs_scan would be something like below:
        column_name    | ordinal   |     data_type     | character
                         position                        maximum_length
        ---------------+-----------+-------------------+----------------
        userid         |         1 | integer           |
        query          |         2 | integer           |
        slice          |         3 | integer           |
        segment        |         4 | integer           |
        step           |         5 | integer           |
        starttime      |         6 | timestamp without |
                                     time zone
        endtime        |         7 | timestamp without |
                                     time zone
        tasknum        |         8 | integer           |
        rows           |         9 | bigint            |
        bytes          |        10 | bigint            |
        tbl            |        11 | integer           |
        is_diskbased   |        12 | character         |   1
        workmem        |        13 | bigint            |
        __path         |        14 | character         | 2050
        __size         |        15 | bigint            |
        __spectrum_oid |        16 | character         | 128
        """
        query = ("SELECT column_name,ordinal_position,"
                 "data_type,character_maximum_length "
                 "FROM information_schema.columns "
                 "WHERE table_schema='pg_catalog' AND "
                 "table_name='{}' ORDER BY ordinal_position;")

        stcs_list_result = self.get_stcs_table_names(cluster)
        for stcs_table in stcs_list_result:
            stcs_log_name = stcs_table[0]
            stll_log_name = stcs_log_name.replace("stcs_", "stll_", 1)
            stll_query = query.format(stll_log_name)
            stcs_query = query.format(stcs_log_name)
            err_msg = '{} and {} are different.'.format(
                stll_log_name, stcs_log_name)
            err_msg2 = '{} returns ZERO columns'.format(stll_log_name)
            with db_session.cursor() as cursor:
                cursor.execute(stll_query)
                stll_res = cursor.fetchall()
                cursor.execute(stcs_query)
                stcs_res = cursor.fetchall()

                stll_col_num = len(stll_res)
                stcs_col_num = len(stcs_res)
                assert (stll_col_num > 0), err_msg2
                # stcs table has two pseudo columns (__path, __size and
                # __spectrum_oid).
                assert ((stll_col_num + 5) == stcs_col_num), err_msg
                for i in range(len(stll_res)):
                    if stll_res[i][2].lower().startswith("timestamp"):
                        assert (stll_res[i][0] == stcs_res[i][0]), err_msg
                        assert (stll_res[i][1] == stcs_res[i][1]), err_msg
                        """
                        timestamp (time) column in stll table
                        should be bigint (long) in stcs
                        """
                        assert (stcs_res[i][2].lower() == "bigint"), err_msg
                    else:
                        assert (stll_res[i] == stcs_res[i]), err_msg
                # pseudo columns(__path, __size and __spectrum_oid) always are
                # the last 3rd, 4th and 5th columns if enabled.
                assert (stcs_res[stcs_col_num -
                                 5][0] == '__path'), stcs_res[stcs_col_num - 2]
                assert (stcs_res[stcs_col_num -
                                 4][0] == '__size'), stcs_res[stcs_col_num - 1]
                assert (stcs_res[stcs_col_num - 3][0] == '__spectrum_oid'
                        ), stcs_res[stcs_col_num - 2]
