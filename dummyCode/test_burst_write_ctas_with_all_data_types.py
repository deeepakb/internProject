# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from contextlib import contextmanager

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.cred_helper import get_key_auth_str
from raff.common.profile import Profiles

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CTAS_CMD = "CREATE TABLE {} AS SELECT * FROM {}"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true'
})
@pytest.mark.custom_local_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASDataTypes(BurstWriteTest):
    @contextmanager
    def create_range_table(self, cursor):
        try:
            # We don't include geography and super data types since
            # these types aren't supported on burst.
            query = """create table if not exists RangeTableNumber (
                            id integer,
                            year integer,
                            name varchar(64),
                            rating varchar(64),
                            default_c0 INT2 DEFAULT 1,
                            default_c1 INT DEFAULT 10,
                            default_c2 BPCHAR DEFAULT '100',
                            default_c3 NVARCHAR DEFAULT '100',
                            default_c4 INT8 DEFAULT 100,
                            default_c5 NUMERIC(8,2) DEFAULT 10.0,
                            default_c6 FLOAT4 DEFAULT 20.0,
                            default_c7 FLOAT8 DEFAULT 100.0,
                            default_c8 BOOL DEFAULT TRUE,
                            default_c9 TIME DEFAULT '07:07',
                            default_c10 TIMETZ DEFAULT '07:07 PST',
                            default_c11 DATE DEFAULT getdate(),
                            default_c12 TIMESTAMP DEFAULT getdate(),
                            default_c13 TIMESTAMPTZ DEFAULT getdate(),
                            default_c14 HLLSKETCH DEFAULT
                                ('{"logm":15,"sparse":{"indices":
                                [4878,9559,14523],"values":[1,2,1]}}'),
                            default_c15 VARBYTE DEFAULT 'abc',
                            default_c16 VARCHAR(50) DEFAULT CURRENT_USER,
                            default_c17 TEXT DEFAULT
                               CONCAT('name'::text, 'rating'::text)
                        )
                        diststyle even"""

            cursor.execute(query)
            yield
        finally:
            cursor.execute("drop table RangeTableNumber")

    def _run_ctas_on_burst(self, cursor, cluster, ctas_table, base_table):
        cursor.execute("set query_group to burst")
        cursor.execute(CTAS_CMD.format(ctas_table, base_table))
        self._check_last_ctas_bursted(cluster)

    def _verify_ctas_table_contents(self, cursor, cluster,
                                    expected_res, ctas_table, base_table):
        # verify table contents on burst and on main
        cursor.execute("set query_group to burst")
        cursor.execute("select * from {} order by 1;".format(ctas_table))
        ctas_result_burst = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        assert expected_res == ctas_result_burst

        cursor.execute("select * from {} order by 1;".format(base_table))
        base_result_burst = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        assert expected_res == base_result_burst

        cursor.execute("set query_group to noburst")
        cursor.execute("select * from {} order by 1;".format(ctas_table))
        ctas_result = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        assert expected_res == ctas_result

        cursor.execute("select * from {} order by 1;".format(base_table))
        base_result = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        assert expected_res == base_result

        # verify content between base table and newly create ctas table
        assert base_result == ctas_result

    def test_burst_write_ctas_with_all_data_types(self, cluster, db_session):
        credential = get_key_auth_str(profile=Profiles.COOKIE_CORE)

        with db_session.cursor() as cursor:
            main_ctas_table = "main_ctas_table"
            ctas_table_0 = "ctas_table_0"
            ctas_table_1 = "ctas_table_1"
            ctas_table_2 = "ctas_table_2"
            base_table = "RangeTableNumber"
            with self.create_range_table(cursor):
                cursor.run_copy(
                    "RangeTableNumber(id,year,name,rating)",
                    "dynamodb://RangeTableNumber",
                    credential,
                    readratio=100,
                    COMPUPDATE="OFF")
                self._start_and_wait_for_refresh(cluster)

                # run ctas on main to get the table to be checked against
                cursor.execute("set query_group to noburst")
                cursor.execute(CTAS_CMD.format(main_ctas_table, base_table))
                cursor.execute("select * from {} order by 1;".format(main_ctas_table))
                expected_res = cursor.fetchall()
                self._check_last_ctas_didnt_burst(cluster)

                self._run_ctas_on_burst(cursor, cluster,
                                        ctas_table_0, base_table)
                # verify table contents
                self._verify_ctas_table_contents(cursor, cluster, expected_res,
                                                 ctas_table_0, base_table)

                self._run_ctas_on_burst(cursor, cluster,
                                        ctas_table_1, base_table)
                # verify table contents
                self._verify_ctas_table_contents(cursor, cluster, expected_res,
                                                 ctas_table_1, ctas_table_0)

                self._run_ctas_on_burst(cursor, cluster,
                                        ctas_table_2, base_table)
                # verify table contents
                self._verify_ctas_table_contents(cursor, cluster, expected_res,
                                                 ctas_table_2, base_table)
