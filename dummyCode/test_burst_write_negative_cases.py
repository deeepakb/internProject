# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.db_exception import ProgrammingError
from raff.common.db.session_context import SessionContext

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = ("INSERT INTO {} select * from {}")
INSERT_CMD = ("INSERT INTO {} values (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, "
              "1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1);")
SELECT_CMD = ("select count(*) from {}")
EFFECTIVE_DISTSTYLE_SQL = ("select releffectivediststyle from "
                           "pg_class_info "
                           "where reloid='{}'::regclass::oid;")

DELETE_CMD = "DELETE FROM {} where {}"

UPDATE_CMD = "UPDATE {} SET {}"

OWNERSHIP_STATE_CHECK_QUERY = (
    "select case when trim(arn) like 'arn%' then 'Burst' else trim(arn) end, "
    "trim(state) from stv_burst_tbl_ownership "
    "where id='{}'::regclass::oid "
    "UNION "
    "select 'Main', 'Undo' from stv_burst_tbl_sb_version "
    "where id='{}'::regclass::oid and undo_sb_version > "
    "(select max(backup_version) from stv_burst_manager_cluster_info)"
    "order by 1, 2;")

COPY_SUPER = (
    "COPY complex_super_test FROM "
    "'s3://cookie-monster-s3-ingestion/json_avro_extended_features/lowercase_keys_nested.json' "
    " IAM_ROLE 'arn:aws:iam::467896856988:role/Redshift-S3-Write' "
    " FORMAT JSON 'auto' COMPUPDATE OFF;")

COPY_GEOMETRY = (
    "COPY nyc_subway_stations(notes,url,name,objectid,line,geom) "
    " FROM 's3://qa-gis/NTT-data/nyc_subway_stations.csv'"
    " CREDENTIALS 'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3' COMPUPDATE OFF;"
)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_user_temp_table': 'false'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'burst_enable_write_super_geo_col': 'false',
        'enable_geography': 'true',
        'spatial_version': '7',
        'burst_enable_user_temp_table': 'false'
    })
class TestBurstWriteSSModeNegativeCases(BurstWriteTest):
    def _verify_effective_diststyle(self, cursor, tbl, diststyle):
        cursor.execute(EFFECTIVE_DISTSTYLE_SQL.format(tbl))
        res = cursor.fetch_scalar()
        log.info("effective diststyle for table %s is %s" % (tbl, res))
        assert res == diststyle

    def _run_copy(self, cursor, tbl):
        cursor.run_copy(
            '{}'.format(tbl),
            's3://tpc-h/tpc-ds/1/catalog_returns.',
            gzip=True,
            delimiter="|",
            COMPUPDATE=True)

    def _check_ownership_no_schema(self, schema, tbl, state, error=False):
        with self.db.cursor() as cursor:
            try:
                cursor.execute("set search_path to {}".format(schema))
                cursor.execute(OWNERSHIP_STATE_CHECK_QUERY.format(tbl, tbl))
            except ProgrammingError as e:
                assert error
                assert 'does not exist' in str(e)
                return
            res = cursor.fetchall()
            assert res == state

    def test_burst_write_negative_cases(self, cluster):
        """
        Tests limitations for using concurrency scaling, covers:
        1. doesn't support COPY/INSERT queries on temporary tables.
           1.1. copy on temp table
           1.2. insert on temp table
        2. doesn't support COPY/INSERT queries on z-indexed tables.
           2.1. copy on z-indexed table
           2.2. insert on z-indexed table
        3. doesn't support COPY/INSERT on dist-auto-all/dist-all table.
           3.1. copy on dist-all
           3.2. insert on dist-all
           3.3. insert on pristine dist-auto-all
           3.4. copy on pristine dist-auto-all
           3.5. insert on non-pristine dist-auto-all
           3.6. insert-select on auto-all with source and target different
           3.7. insert-select on auto-all with source and target the same
           3.8. insert-select with source table auto-all and target not
                note this case should burst.
           3.9. copy on non-pristine dist-auto-all
        5. doesn't support queries that accesses table with identity columns
        6. doesn't support compupdate query on pristine tables.
        7. doesn't support when target table has super columns.

        Note case 8 - 10 has been covered in test_burst_rules_log
        8. doesn't support queries that contain Python user-defined
           functions (UDF). Since UDFs are basically restricted to
           what do inside a SELECT clause, we can skip testing write
           queries with UDF.
        9. ANALYZE is not supported.
        10. doesn't support queries that access system tables PG tables
           (As Views cannot be loaded with the COPY command, skip testing
            COPY on PG tables;
            Manually checked insert on PG tables is not bursted.
            As in ss mode, only set session authorization 'master',
            can the query be bursted. However, with this authorization, dml
            on pg tables is not permitted. so skip this case in this test.)
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file(
            "create_diverse_tables_for_burst_write", session=db_session)

        with db_session.cursor() as cursor:
            # Case 1: Query on temp table should not be bursted.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("select count(*) from catalog_returns_temp;")
            catalog_returns_size = cursor.fetch_scalar()
            self.check_last_query_didnt_burst_status(cluster, cursor, 5)

            # Case 1.1: COPY should not be bursted on temp table.
            log.info("check copy on temp table")
            self._run_copy(cursor, 'catalog_returns_temp')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 5)
            catalog_returns_size = catalog_returns_size + copy_rows
            cursor.execute("select count(*) from catalog_returns_temp;")
            assert cursor.fetch_scalar() == catalog_returns_size

            # Case 1.2: INSERT on temp table is not bursted.
            log.info("check insert on temp table")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                INSERT_SELECT_CMD.format('catalog_returns_temp',
                                         'catalog_returns_temp'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 5)

            # Case 2: query z-index on temp table is not bursted.
            # Case 2.1: COPY on z-indexed table should not be bursted.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                SELECT_CMD.format("catalog_returns_with_interleaved_sortkey"))
            catalog_returns_size = cursor.fetch_scalar()
            self.check_last_query_didnt_burst_status(cluster, cursor, 8)
            log.info("check copy on z-indexed table")
            self._run_copy(cursor, 'catalog_returns_with_interleaved_sortkey')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 8)
            catalog_returns_size = catalog_returns_size + copy_rows
            cursor.execute(
                SELECT_CMD.format("catalog_returns_with_interleaved_sortkey"))
            assert cursor.fetch_scalar() == catalog_returns_size

            # Case 2.2: INSERT on z-indexed table should not be bursted.
            self._start_and_wait_for_refresh(cluster)
            log.info("check insert into a z-indexed table.")
            cursor.execute(
                INSERT_SELECT_CMD.format(
                    'catalog_returns_with_interleaved_sortkey',
                    'catalog_returns_with_interleaved_sortkey'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 8)

            # Case 3: COPY/INSERT on distall/dist-auto-all shouldnt be bursted.
            # Case 3.1: copy on dist-all table is not bursted.
            log.info("check copy on dist-all table")
            self._start_and_wait_for_refresh(cluster)
            self._run_copy(cursor, 'catalog_returns_distall')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 48)

            # Case 3.2: insert on dist-all table is not bursted.
            log.info("check insert on dist-all table")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                INSERT_SELECT_CMD.format('catalog_returns_distall',
                                         'catalog_returns_distall'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 48)

            # Case 3.3: insert into pristine dist-auto-all table
            # is not bursted.
            self._start_and_wait_for_refresh(cluster)
            log.info("check insert on pristine auto-all table")
            # `effective diststyle = 10` represents dist-auto-all
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_1', 10)
            cursor.execute(INSERT_CMD.format('catalog_returns_distautoall_1'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 48)

            # Case 3.4: copy on pristine dist-auto-all table is not bursted.
            self._start_and_wait_for_refresh(cluster)
            log.info("check copy on pristine dist-auto-all table")
            # `effective diststyle = 10` represents dist-auto-all
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_2', 10)
            self._run_copy(cursor, 'catalog_returns_distautoall_2')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 49)

            # Case 3.5: insert on non-pristine dist-auto-all table
            # is not bursted.
            self._start_and_wait_for_refresh(cluster)
            log.info("check insert on dist-auto-all table")
            # `effective diststyle = 10` represents dist-auto-all
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_3', 10)
            cursor.execute(INSERT_CMD.format('catalog_returns_distautoall_3'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 48)

            # Case 3.6: insert-select on non-pristine auto-all table
            # with source table different from target table.
            log.info("check insert select on dist-auto-all table")
            self._start_and_wait_for_refresh(cluster)
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_3', 10)
            cursor.execute(
                INSERT_SELECT_CMD.format('catalog_returns_distautoall_3',
                                         'catalog_returns'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 48)

            # Case 3.7: insert-select on non-pristine auto-all table
            # with source table the same with target table.
            log.info("check insert select on dist-auto-all table")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                INSERT_SELECT_CMD.format('catalog_returns_distautoall_5',
                                         'catalog_returns_distautoall_5'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 48)

            # Case 3.8: insert into t1 select * from t2, where t2 is auto-all
            # and t1 is auto-even. should burst.
            self._start_and_wait_for_refresh(cluster)
            self._verify_effective_diststyle(cursor, 'catalog_returns', 11)
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_4', 10)
            cursor.execute(
                INSERT_SELECT_CMD.format('catalog_returns',
                                         'catalog_returns_distautoall_4'))
            self._check_last_query_bursted(cluster, cursor)

            # Case 3.9: copy on non-pristine dist-auto-all table
            # is not bursted.
            self._start_and_wait_for_refresh(cluster)
            log.info("check copy on non-pristine dist-auto-all table")
            # `effective diststyle = 10` represents dist-auto-all
            self._verify_effective_diststyle(
                cursor, 'catalog_returns_distautoall_4', 10)
            self._run_copy(cursor, 'catalog_returns_distautoall_4')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 49)

            # Case 5: copy/insert/delete/update wont burst on table with
            # identity column.
            log.info("check tables with identity")
            self._run_copy(cursor, 'catalog_returns_identity')
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 144067
            self.check_last_copy_not_bursted_status(cluster, cursor, 52)
            # test if target table does not have id col, can burst
            cursor.execute(
                "insert into catalog_returns_no_id select * from catalog_returns_identity"
            )
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                "insert into catalog_returns_no_id select * from catalog_returns_identity"
            )
            self._check_last_query_bursted(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_CMD.format('catalog_returns_identity'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 52)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                DELETE_CMD.format('catalog_returns_identity',
                                  'cr_returned_time_sk < 3'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 52)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                UPDATE_CMD.format('catalog_returns_identity',
                                  'cr_returned_time_sk = 5'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 52)

            # Case 6: Copy compression update can not burst on pristine.
            log.info("check query with compression update")
            cursor.execute(
                "create table table_update as select * from catalog_returns;")
            self._start_and_wait_for_refresh(cluster)
            self._run_copy(cursor, 'table_update')
            # copy compupdate works on non-pristine table because compupdate
            # is ignored for non-pristine.
            self.check_last_query_bursted(cluster, cursor)
            cursor.execute("truncate table_update;")
            self._start_and_wait_for_refresh(cluster)
            self._run_copy(cursor, 'table_update')
            # Error code 3 means ineligible query type
            self.check_last_query_didnt_burst_status(cluster, cursor, 3)

            # Case 6: alter view rename should not be owned, but can burst
            # because it is not a true table and will not get reset
            log.info("check query with a view")
            schema = db_session.session_ctx.schema
            cursor.execute(
                "create view return_view as select * from catalog_returns;")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_CMD.format('catalog_returns'))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                "alter table return_view rename to return_view_new;")
            self._check_last_query_bursted(cluster, cursor)
            self._check_ownership_no_schema(schema, 'return_view', None, True)
            self._check_ownership_no_schema(schema, 'return_view_new', [])
            cursor.execute(INSERT_CMD.format('catalog_returns'))
            self._check_last_query_bursted(cluster, cursor)
            self._check_ownership_no_schema(schema, 'catalog_returns',
                                            [('Burst', 'Owned')])

            # Case 8: Write queries with target table having super columns cannot burst.
            # Case 8.1: Copy on table with super columns cannot burst.
            log.info("check copy on table with super col")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(COPY_SUPER)
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 11
            self.check_last_copy_not_bursted_status(cluster, cursor, 64)
            cursor.execute("select count(*) from complex_super_test;")
            assert cursor.fetch_scalar() == copy_rows

            # Case 8.2: Insert on table with super columns cannot burst.
            log.info("check insert on table with super col")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                INSERT_SELECT_CMD.format('complex_super_test',
                                         'complex_super_test'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 64)

            # Case 8.3: select on table with super columns can burst.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("select count(*) from complex_super_test;")
            super_test_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            assert super_test_size == 22

            # Case 9: Write queries with target table having geometry columns cannot burst.
            # Case 9.1: Copy on table with geometry columns cannot burst.
            log.info("check copy on table with geometry col")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(COPY_GEOMETRY)
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 473
            self.check_last_copy_not_bursted_status(cluster, cursor, 64)
            cursor.execute("select count(*) from nyc_subway_stations;")
            assert cursor.fetch_scalar() == copy_rows

            # Case 9.2: Insert on table with geometry columns cannot burst.
            log.info("check insert on table with geometry col")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                INSERT_SELECT_CMD.format('nyc_subway_stations',
                                         'nyc_subway_stations'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 64)

            # Case 9.3: select on table with geometry columns can burst.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("select count(*) from nyc_subway_stations;")
            super_test_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            assert super_test_size == 946

            # Case 10: Write on table with GEOGRAPHY columns cannot burst.
            # Case 10.1: Insert on table with geography columns cannot burst.
            log.info("check insert on table with geography col")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_SELECT_CMD.format('geo_table', 'geo_table'))
            self.check_last_query_didnt_burst_status(cluster, cursor, 64)
            # Case 10.2: Select on table with geography columns can burst.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("select count(*) from geo_table;")
            super_test_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            assert super_test_size == 2
