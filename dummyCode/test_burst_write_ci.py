# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import string

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions

__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCaseInsensitive(BurstWriteTest):
    def test_ci_columns(self, cluster, db_session):
        """
        This test port the basic case insensitive column test to burst
        write environment.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            # Setup tables.
            self.execute_test_file(
                'test_burst_ci_basic_setup', session=db_session)
            self._start_and_wait_for_refresh(cluster)
            # Runs burst write queries on case insensitive tables.
            cursor.execute("set query_group to burst;")
            self._run_yaml_test(
                    cluster, db_session, 'test_burst_ci_basic_dml', 7)
            # Check case insensitive tables contents after burst write on
            # burst cluster.
            self._run_yaml_test(
                    cluster, db_session, 'burst_column_collation', 49)
            # Check case insensitive tables contents after burst write on
            # main cluster.
            cursor.execute("set query_group to metrics;")
            self.execute_test_file(
                'burst_column_collation', session=db_session)
            # Update table through case insensitive column in burst cluster.
            cursor.execute("set query_group to burst;")
            cursor.execute("update dp25176 set ci_varchar = ci_varchar || 'a',"
                           "cs_varchar = cs_varchar || 'a', "
                           "ci_char = ci_char || 'A', "
                           "cs_char = cs_char || 'A', "
                           "cint = cint + 10, "
                           "cbigint = cbigint + 100 "
                           "where ci_varchar ='AmAzOn';")
            self._check_last_query_bursted(cluster, cursor)

            full_table_scan = ("select trim(ci_varchar), trim(cs_varchar), "
                               "trim(ci_char), trim(cs_char), cint, cbigint "
                               "from dp25176;")
            cursor.execute(full_table_scan)
            res = cursor.fetchall()
            assert res == [
            ('amazona', 'AMAZONa', 'hello worldA', 'hELLO worldA', 10, 100),
            ('Amazona', 'amaZONa', 'hello WOrldA', 'hellO WorldA', 11, 101),
            ('AMAZONa', 'AMAzona', 'HELLo worldA', 'HELLO worldA', 12, 102),
            ('AmaZona', 'amaZona', 'heLLo WOrlDA', 'Hello WorldA', 13, 103)
            ]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("update dp25176 set ci_varchar = ci_varchar || 'A',"
                           "cs_varchar = cs_varchar || 'A', "
                           "ci_char = ci_char || 'a', "
                           "cs_char = cs_char || 'a', "
                           "cint = cint + 10, "
                           "cbigint = cbigint + 100 "
                           "where ci_varchar ='AmAzOnA'")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(full_table_scan)
            res = cursor.fetchall()
            assert res == [
            ('amazonaA', 'AMAZONaA', 'hello worldAa', 'hELLO worldAa', 20, 200),
            ('AmazonaA', 'amaZONaA', 'hello WOrldAa', 'hellO WorldAa', 21, 201),
            ('AMAZONaA', 'AMAzonaA', 'HELLo worldAa', 'HELLO worldAa', 22, 202),
            ('AmaZonaA', 'amaZonaA', 'heLLo WOrlDAa', 'Hello WorldAa', 23, 203)]
            self._check_last_query_bursted(cluster, cursor)


    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)', 'distkey(c1)',
                            'distkey(c2)', 'distkey(c3)'],
                sortkey=['', 'sortkey(c0)', 'sortkey(c1)', 'sortkey(c2)',
                         'sortkey(c3)']))

    def _setup_tables(self, cursor, schema, vector):
        cursor.execute('''
                create table t1(c0 varchar(32) COLLATE case_insensitive,
                c1 varchar(32) COLLATE case_sensitive,
                c2 char(32) COLLATE case_insensitive,
                c3 char(32) COLLATE case_sensitive,
                c4 bigint) {} {}'''.format(
                vector.diststyle, vector.sortkey))

    def test_burst_write_ci_tables(self, cluster, db_session, vector):
        """
        This test set up table with case sensitive and case insensitive columns
        with different diststyle and sortkey. Then, burst select, insert,
        delete, and update statements on this table on case insensitive columns
        to check the table can handle case insensitive correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._setup_tables(cursor, schema, vector)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute('set query_group to burst;')
            self._run_yaml_test(
                    cluster, db_session, 'test_burst_write_ci_tables', 20)
            self._validate_table(cluster, schema, 't1', vector.diststyle)
            ins_select = ("insert into t1 select c0 || 'a', c1 || 'a', "
                          "c2 || 'A', c3 || 'A', c4 + 10 from t1;");
            cursor.execute('begin;')
            for i in range(8):
                cursor.execute(ins_select)
            cursor.execute('commit;')
            cursor.execute('vacuum t1 to 100 percent;')
            self._validate_table(cluster, schema, 't1', vector.diststyle)
            self._start_and_wait_for_refresh(cluster)
            for c in string.ascii_lowercase:
                key = c * 16
                if key > 'TTTTTTTT':
                    continue
                cursor.execute('set query_group to metrics;')
                cursor.execute("select count(*) from c0 = '{}';".format(key))
                assert 1024 == cursor.fetch_scalar()
                cursor.execute('set query_group to burst;')
                cursor.execute("select count(*) from c0 = '{}';".format(key))
                assert 1024 == cursor.fetch_scalar()
                self._check_last_query_bursted(cluster, cursor)
                cursor.execute("delete from t1 where c0 = '{}';".format(key))
                self._check_last_query_bursted(cluster, cursor)

                cursor.execute('set query_group to metrics;')
                cursor.execute("select count(*) from c2 = '{}';".format(key))
                assert 0 == cursor.fetch_scalar()
                cursor.execute('set query_group to burst;')
                cursor.execute("select count(*) from c2 = '{}';".format(key))
                assert 0 == cursor.fetch_scalar()
                self._check_last_query_bursted(cluster, cursor)
