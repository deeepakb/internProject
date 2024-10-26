# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_test import get_burst_cluster_name
from raff.common.burst_helper import (get_cluster_by_identifier,
                                      get_cluster_by_arn, identifier_from_arn)
from raff.common.host_type import HostType

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = "INSERT INTO {sch}.{tbl} SELECT * FROM {sch}.{tbl}"
CREATE_STMT = "CREATE TABLE {}.{} (c0 int, c1 int) {} {}"
INSERT_CMD = "INSERT INTO {}.{} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteDropRename(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(diststyle=['diststyle even'], sortkey=['']))

    def _setup_tables(self, db_session, schema, base_tbl_name, vector):
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(schema, base_tbl_name, vector.diststyle,
                                   vector.sortkey))
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            for i in range(8):
                cursor.execute(
                    INSERT_SELECT_CMD.format(sch=schema, tbl=base_tbl_name))

    def test_burst_write_drop_table(self, cluster, vector):
        """
        Test: after drop table on main, write query on burst will fail.
        1. bursted write;
        2. drop table on main;
        3. write on burst cluster should fail;
        4. re-create table with same name;
        5. write cannot be bursted;
        6. refresh;
        7. write can be bursted;
        """

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema

        base_tbl_name = 'dp28928_t1'
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            self._setup_tables(db_session_master, schema, base_tbl_name,
                               vector)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("set query_group to metrics;")
            cursor.execute("drop table {}.{}".format(schema, base_tbl_name))
            # after drop table on main, bursted write should fail.
            cursor.execute("set query_group to burst;")
            try:
                cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            except Exception as e:
                log.info("error msg is {}".format(e))
                pass
            # recreate a table with same name, write on this table shouldnt burst.
            self._setup_tables(db_session_master, schema, base_tbl_name,
                               vector)
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_didnt_burst(cluster, cursor)

            # refresh then write can be bursted.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)

    def test_burst_write_drop_schema(self, cluster, vector):
        """
        Test: after drop schema on main and recreate one with same name,
              write query cannot be bursted without refresh.
        1. bursted write;
        2. drop schema on main;
        3. write set to burst on the table should fail
        4. re-create schema with same name;
        5. re-create table with same name;
        6. write cannot be bursted;
        7. refresh;
        8. write can be bursted;
        """
        schema = 'test_schema'
        session_ctx = SessionContext(schema=schema)
        base_tbl_name = 'dp28928_t1'
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx) as session1:
            cursor = session1.cursor()
            cursor.execute("set query_group to burst;")
            self._setup_tables(session1, schema, base_tbl_name, vector)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)
            # drop schema
            cursor.execute("set query_group to metrics;")
            cursor.execute("drop schema {} CASCADE".format(schema))
            cursor.execute("set query_group to burst")
            try:
                cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            except Exception as e:
                log.info("error msg is {}".format(e))
                pass
            cursor.execute("create schema {}".format(schema))
            cursor.execute(
                CREATE_STMT.format(schema, base_tbl_name, vector.diststyle,
                                   vector.sortkey))
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_didnt_burst(cluster, cursor)
            # refresh then write can be bursted.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)

    def test_burst_write_rename_schema(self, cluster, vector):
        """
        Test: after rename schema, write can still be bursted, but
        write on a recreated schema with same name couldnt be bursted
        until refresh.
        1. bursted write on sh1.t1;
        2. rename schema sh1 to sh2 on main;
        3. write on sh2.t1 can still be bursted;
        4. re-create schema with same name (sh1);
        5. re-create table with same name (sh1.t1);
        6. write on sh1.t1 cannot be bursted;
        7. refresh;
        8. write on sh1.t1 can be bursted;
        """

        schema = 'test_schema'
        session_ctx = SessionContext(schema=schema)
        base_tbl_name = 'dp28928_t1'
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx) as session1:
            cursor = session1.cursor()
            cursor.execute("set query_group to burst;")
            self._setup_tables(session1, schema, base_tbl_name, vector)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)
            # rename schema
            cursor.execute("set query_group to metrics;")
            cursor.execute("alter schema {} rename to {}".format(
                schema, schema + "_rename"))
            # after rename schema, write can still be bursted on this table.
            cursor.execute("set query_group to burst")
            cursor.execute(
                INSERT_CMD.format(schema + "_rename", base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("create schema {}".format(schema))
            cursor.execute(
                CREATE_STMT.format(schema, base_tbl_name, vector.diststyle,
                                   vector.sortkey))
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_didnt_burst(cluster, cursor)
            # refresh then write can be bursted.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT_CMD.format(schema, base_tbl_name))
            self._check_last_query_bursted(cluster, cursor)
