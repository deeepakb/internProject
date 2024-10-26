# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging

from raff.data.data_utils import load_table
from raff.burst.burst_test import BurstTest
from raff.common.db.redshift_db import RedshiftDb
from raff.common.cluster.cluster_session import ClusterSession
log = logging.getLogger(__name__)

SB_VERSION_LOOKUP = "SELECT DISTINCT sb_version FROM stv_superblock;"

CREATE_TABLE = ('CREATE TABLE {} '
                '(id int not null, comment varchar(20), extra_col bigint, '
                'primary key(id));')

# Create table with a specified diststyle.
CREATE_TABLE_DIST = (
    'CREATE TABLE {} '
    '(id int not null, comment varchar(20), extra_col bigint, '
    'primary key(id)) {};')

DROP_TABLE = 'DROP TABLE IF EXISTS {};'
INSERT_TABLE = ('INSERT INTO {} values ({})')
# This macro is picked from padb code. Each LN query gets assigned a slice
# ranging from FIRST_LEADER_SLICE to LAST_LEADER_SLICE. Please check
# commslice.hpp for more details.
FIRST_LEADER_SLICE = 6411

ALTER_DISTAUTO = "alter table {} alter diststyle auto"
ALTER_DISTEVEN = "alter table {} alter diststyle even"
ATC_TABLE_NAME = 'customer_atc_giant_table'


@pytest.yield_fixture(scope='class')
def alter_command_gucs(cluster):
    session = ClusterSession(cluster)
    gucs = {
        "enable_alter_column_type": "true",
        "enable_alter_column_type_dec": "true",
        "enable_alter_distkey": "true",
        "enable_alter_distkey_on_distall": "true",
        "enable_alter_dist_sort": "true",
        "enable_alter_distauto": "true",
        "enable_alter_disteven": "true",
        "vacuum_auto_worker_enable": "false",
        "small_table_row_threshold": 20,
        "small_table_row_lower_bound": 1,
        "small_table_block_threshold": 2,
        "alter_diststyle_scan_wlock_threshold": 10,
        # enable_atd_checkpoint_gucs
        "enable_alter_diststyle_multi_table_checkpoint": "true",
        "alter_diststyle_checkpoint_cn_threshold": "10",
        "atw_checkpoint_table_multiplier": "50",
        "auto_analyze": "false",
        "enable_catalog_rebuild_worker": "false",
        "enable_redcat_pgclass_mirror": "false"
    }

    with session(gucs):
        yield


@pytest.yield_fixture(scope='class')
def disable_all_autoworkers(alter_command_gucs, cluster):
    cluster.run_xpx('auto_worker disable all')
    yield
    cluster.run_xpx('auto_worker enable all')

"""
Marking this class as no-jdbc due to issue
https://issues.amazon.com/issues/RedshiftDP-56145
"""


@pytest.mark.no_jdbc
@pytest.mark.localhost_only
@pytest.mark.usefixtures("alter_command_gucs")
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.usefixtures("disable_all_autoworkers")
class TestTableDDLVersion(BurstTest):
    """
    Test suite that ensures ddl_end_version_ is being populated in Table_perm
    upon DDL commit/rollback.
    """

    def _get_current_sb_version(self):
        """
        Get current superblock version.

        Returns:
            current super block version integer
        """
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SB_VERSION_LOOKUP)
            result = bootstrap_cursor.fetch_scalar()
        return result

    def _validate_table_ddl_version(self, table_name, ddl_end_version):
        """
        Validates the ddl_end_version populated in Table_perm for an input
        table.
        Args:
            table_name (str): name of the table
            ddl_end_version (long): expected ddl_end_version
        """

        query = ("SELECT ddl_end_version FROM stv_tbl_perm_property WHERE id ="
                 " (select oid from pg_class where relname = '{}') AND"
                 " slice >= {}").format(table_name, FIRST_LEADER_SLICE)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(query)
            end_version = bootstrap_cursor.fetch_scalar()
        assert int(end_version) == ddl_end_version

    def _run_query_and_validate_version(self, session, table_name, query,
                                        is_commit, expected_ddl_version):
        """
        This routine does the following steps:
        1. Runs the input <query> in the input <session>.
        2. Commits the session or Rollsback based on input flag <is_commit>.
        3. Validates ddl_end_version for the input <table_name>.

        Args:
            session (RedshiftDb): Session object.
            table_name (str): Name of the table.
            query (str): Query to run in the session.
            is_commit (bool): Decides to commit or rollback the input session.
            expected_ddl_end_version (long): Expected ddl_end_version.
        """

        with session.cursor() as cursor:
            cursor.execute(query)
        if is_commit:
            session.commit()
        else:
            session.rollback()
        self._validate_table_ddl_version(table_name, expected_ddl_version)

    def test_table_ddl_end_version(self, cluster):
        """
        This test ensures ddl_end_version is populated correctly on
        COMMIT/ROLLBACK of Create/Drop table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a table
        table_name = 'table_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
        session.commit()
        sb_version = self._get_current_sb_version()

        # Create table - Commit
        query1 = CREATE_TABLE.format(table_name)
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())
        # Drop table - Rollback
        # Note that undoing of a drop table, does not increase ddl_end_version.
        # Superblock version increments upon rollback with commit_on_sync_undo
        # GUC enabled
        query2 = DROP_TABLE.format(table_name)
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 2 == self._get_current_sb_version())

    def test_column_ddl_end_version(self, cluster):
        """
        This test ensures ddl_end_version is populated correctly on
        COMMIT/ROLLBACK of Alter table Add/Drop Column.
        Note that - The superblock version increments on rollback with
        commit_on_sync_undo GUC enabled.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a table
        table_name = 'table_column_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE.format(table_name))
        session.commit()

        sb_version = self._get_current_sb_version()

        # Alter table add column - Commit
        query1 = "ALTER TABLE table_column_ddl ADD COLUMN abc int"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table add column - Rollback
        query2 = "ALTER TABLE table_column_ddl ADD COLUMN cba int"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())

        # Alter table drop column - Commit
        query3 = "ALTER TABLE table_column_ddl DROP COLUMN abc"
        self._run_query_and_validate_version(session, table_name, query3, True,
                                             sb_version + 3)
        assert (sb_version + 3 == self._get_current_sb_version())

        # Alter table drop column - Rollback
        query4 = "ALTER TABLE table_column_ddl DROP COLUMN comment"
        self._run_query_and_validate_version(session, table_name, query4,
                                             False, sb_version + 4)
        assert (sb_version + 4 == self._get_current_sb_version())

        # Alter table alter distkey - Commit
        query5 = "ALTER TABLE table_column_ddl alter distkey comment"
        self._run_query_and_validate_version(session, table_name, query5, True,
                                             sb_version + 5)
        assert (sb_version + 5 == self._get_current_sb_version())

        # Alter table alter diststyle alter sortkey - Rollback
        query6 = "ALTER TABLE table_column_ddl alter diststyle all,\
                  alter sortkey(comment);"

        self._run_query_and_validate_version(session, table_name, query6,
                                             False, sb_version + 6)
        assert (sb_version + 6 == self._get_current_sb_version())

        # Alter table alter disstyle alter sortkey - Commit
        query6 = "ALTER TABLE table_column_ddl alter diststyle all,\
                  alter sortkey(comment);"

        self._run_query_and_validate_version(session, table_name, query6, True,
                                             sb_version + 7)
        assert (sb_version + 7 == self._get_current_sb_version())

        # Alter table alter distkey alter sortkey - Rollback
        query7 = "ALTER TABLE table_column_ddl alter distkey comment,\
                  alter sortkey(id, comment)"

        self._run_query_and_validate_version(session, table_name, query7,
                                             False, sb_version + 8)
        assert (sb_version + 8 == self._get_current_sb_version())

        # Alter table alter distkey alter sortkey - Commit
        query7 = "ALTER TABLE table_column_ddl alter distkey comment,\
                  alter sortkey(id, comment)"

        self._run_query_and_validate_version(session, table_name, query7, True,
                                             sb_version + 9)
        assert (sb_version + 9 == self._get_current_sb_version())

        # Alter table alter distkey - Rollback
        query8 = "ALTER TABLE table_column_ddl alter distkey extra_col"
        self._run_query_and_validate_version(session, table_name, query8,
                                             False, sb_version + 10)
        assert (sb_version + 10 == self._get_current_sb_version())

        # Alter table alter distkey - Commit
        query9 = "ALTER TABLE table_column_ddl alter distkey extra_col"
        self._run_query_and_validate_version(session, table_name, query9, True,
                                             sb_version + 11)
        assert (sb_version + 11 == self._get_current_sb_version())

        # Alter column type command does not support transaction right now.
        # To simulate rollback for alter column type, it needs a rollback
        # event named EtAlterColumnRollBackTest.
        # The event would error out alter command when it has completed in
        # both LN and CNs.
        session.autocommit = True
        cluster.unset_event("EtAlterColumnSimulateRollBackTest")
        # Alter table alter column type - Commit
        query10 = ("ALTER TABLE table_column_ddl ALTER COLUMN comment TYPE "
                   "varchar(30)")
        self._run_query_and_validate_version(session, table_name, query10,
                                             True, sb_version + 12)
        assert (sb_version + 12 == self._get_current_sb_version())

        # Alter table alter column type - Rollback
        query11 = ("ALTER TABLE table_column_ddl ALTER COLUMN comment TYPE "
                   "varchar(20)")
        cluster.set_event("EtAlterColumnSimulateRollBackTest")
        try:
            self._run_query_and_validate_version(session, table_name, query11,
                                                 False, sb_version + 13)
        except Exception:
            log.info("Alter column type roll back")
        assert (sb_version + 13 == self._get_current_sb_version())

    def _insert_tables(self, cursor, table_name, num=30):
        for i in range(num):
            cursor.execute(
                INSERT_TABLE.format(table_name, "{}, 'my-varchar{}'".format(
                    i, i + 1)))

    def test_alter_distauto_disteven_ddl_end_version(self, cluster):
        """
        This test ensures ddl_end_version is populated correctly on
        COMMIT/ROLLBACK of Alter table Alter Diststyle Auto/Even.
        """
        """
        Test1: `alter diststyle auto` on small dist-all table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a small dist-all table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle all'))
        session.commit()
        log.info("Testing `alter diststyle auto on small dist-all table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test2: `alter diststyle auto` on large dist-all table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a large dist-all table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle all'))
            self._insert_tables(cursor, table_name)
        session.commit()

        log.info("Testing `alter diststyle auto on large dist-all table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test3: `alter diststyle auto` on small dist-even table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a small dist-even table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle even'))
        session.commit()
        log.info("Testing `alter diststyle auto on small dist-even table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test4: `alter diststyle auto` on large dist-even table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a large dist-even table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle even'))
            self._insert_tables(cursor, table_name)
        session.commit()
        log.info("Testing `alter diststyle auto on large dist-even table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test5: `alter diststyle auto` on small dist-key table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a small dist-key table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE_DIST.format(table_name, 'distkey(id)'))
        session.commit()
        log.info("Testing `alter diststyle auto on small dist-key table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test6: `alter diststyle auto` on large dist-key table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a large dist-key table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE_DIST.format(table_name, 'distkey(id)'))
            self._insert_tables(cursor, table_name)
        session.commit()
        log.info("Testing `alter diststyle auto on large dist-key table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle auto - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle auto - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test7: `alter diststyle even` on dist-all table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a dist-all table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle all'))
            self._insert_tables(cursor, table_name)
        session.commit()
        log.info("Testing `alter diststyle even on dist-all table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle even - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle even - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test8: `alter diststyle even` on dist-key table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a dist-key table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE_DIST.format(table_name, 'distkey(id)'))
            self._insert_tables(cursor, table_name)
        session.commit()
        log.info("Testing `alter diststyle even on dist-key table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle even - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle even - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test9: `alter diststyle even` on auto-all table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a auto-all table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle auto'))
        session.commit()
        log.info("Testing `alter diststyle even on auto-all table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle even - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle even - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test10: `alter diststyle even` on auto-even table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a auto-even table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(
                CREATE_TABLE_DIST.format(table_name, 'diststyle even'))
            self._insert_tables(cursor, table_name)
            cursor.execute("ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO")
        session.commit()
        log.info("Testing `alter diststyle even on auto-even table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle even - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle even - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())
        """
        Test11: `alter diststyle even` on auto-key table.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a auto-key table
        table_name = 'table_dist_ddl'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE_DIST.format(table_name, 'distkey(id)'))
            self._insert_tables(cursor, table_name)
            cursor.execute("ALTER TABLE table_dist_ddl ALTER DISTSTYLE AUTO")
        session.commit()
        log.info("Testing `alter diststyle even on auto-key table.")
        sb_version = self._get_current_sb_version()

        # Alter table alter diststyle even - Rollback
        query2 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query2,
                                             False, sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        # Alter table alter diststyle even - Commit
        query1 = "ALTER TABLE table_dist_ddl ALTER DISTSTYLE EVEN"
        self._run_query_and_validate_version(session, table_name, query1, True,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())

    def test_special_table_ops(self, cluster, db_session):
        """
        This test ensures ddl_end_version is captured correctly for table
        operations that don't respect transactionality i.e. Truncate and ATA.
        """

        # Setup a table
        table_name = 'table_special_ops'
        table_name2 = 'table_ata_source'
        with db_session.cursor() as cursor:
            cursor.execute(CREATE_TABLE.format(table_name))
            cursor.execute(CREATE_TABLE.format(table_name2))
            cursor.execute(INSERT_TABLE.format(table_name2, "1, 'my-varchar'"))
            sb_version = self._get_current_sb_version()
            # Truncate
            query1 = "TRUNCATE table_special_ops"
            self._run_query_and_validate_version(db_session.session_db,
                                                 table_name, query1, True,
                                                 sb_version + 1)
            assert sb_version + 1 == self._get_current_sb_version(), (
                "Unexpected superblock version after Truncate")

            sb_version2 = self._get_current_sb_version()

            # ATA
            query2 = ("ALTER TABLE table_special_ops APPEND FROM "
                      "table_ata_source")
            self._run_query_and_validate_version(db_session.session_db,
                                                 table_name2, query2, True,
                                                 sb_version2 + 1)
            assert sb_version2 + 1 == self._get_current_sb_version(), (
                "Unexpected superblock version after ATA")

    def test_small_table_ddl_end_version(self, cluster):
        """
        This test ensures ddl_end_version is populated correctly on
        small table conversion.
        """
        session = RedshiftDb(cluster.get_conn_params())
        session.autocommit = False
        # Setup a table
        table_name = 'small_table'
        with session.cursor() as cursor:
            cursor.execute(DROP_TABLE.format(table_name))
        session.commit()
        sb_version = self._get_current_sb_version()

        # Create table - Commit
        query = CREATE_TABLE.format(table_name)
        self._run_query_and_validate_version(session, table_name, query, True,
                                             sb_version + 1)
        assert (sb_version + 1 == self._get_current_sb_version())

        query = "insert into small_table select * from small_table;"
        self._run_query_and_validate_version(session, table_name, query, False,
                                             sb_version + 2)
        assert (sb_version + 2 == self._get_current_sb_version())

        self._run_query_and_validate_version(session, table_name, query, True,
                                             sb_version + 3)
        assert (sb_version + 3 == self._get_current_sb_version())

    def _setup_data(self, db_session):
        """
        Create the load the data to be used in the test.

        Args:
            db_session: db_session object
        """
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='_atc_giant_table',
            grant_to_public=True)
        with db_session.cursor() as cursor:
            cursor.execute(ALTER_DISTEVEN.format(ATC_TABLE_NAME))
            cursor.execute(ALTER_DISTAUTO.format(ATC_TABLE_NAME))

    def test_burst_alter_distkey_checkpoint_ddl_end_version(self, cluster):
        """
        Test runs ATW Alter Distkey Checkpoint on a table and verify
        ddl_end_version does not change between iterations and does change
        after completion.
        """
        session = RedshiftDb(cluster.get_conn_params())
        with session.cursor() as cursor:
            self._setup_data(session)
            cursor.execute("xpx 'event set EtAlterSimulateCheckpoint'")
            sb_version = self._get_current_sb_version()
            iterations_to_complete = 6
            for _ in range(iterations_to_complete):
                self._validate_table_ddl_version(ATC_TABLE_NAME, sb_version)
                cursor.execute('alter table {} alter distkey {}'.format(
                    ATC_TABLE_NAME, 'c_name'))
            self._validate_table_ddl_version(
                ATC_TABLE_NAME, sb_version + iterations_to_complete)
            cursor.execute("drop table if exists {}".format(ATC_TABLE_NAME))
