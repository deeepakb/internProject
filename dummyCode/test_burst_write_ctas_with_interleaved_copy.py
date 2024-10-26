# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass
import random

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.data_loaders.common import MIN_ALL_SHAPES_CREATE_SQL_FILE, \
                                     get_min_all_shapes_table_list
from raff.common.profile import AwsAccounts
from io import open

S3_PATH = ("s3://padb-all-shapes-dataset/10mb/no_nulls"
           "/all_shapes_data_no_nulls_small_")
DP_IAM_ROLE_ARN = AwsAccounts.DP.iam_roles.Redshift_S3.arn
COPY = ("COPY {} FROM '{}'  IAM_ROLE '{}' delimiter ',' REMOVEQUOTES;")
BASE_TABLE1 = "base_table1"
BASE_TABLE2 = "base_table2"
log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.custom_local_gucs(
        gucs={'ctas_auto_analyze': 'false',
              'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASInterleavedWithCopy(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(DDL_CMD=["CREATE TABLE ctas_table AS SELECT * FROM {}",
                          "SELECT * INTO ctas_table FROM {}"]))

    def _get_random_all_shapes_tables(self):
        table_list = get_min_all_shapes_table_list()
        return random.choice(table_list)

    def _setup_base_tables(self, cursor):
        with open(MIN_ALL_SHAPES_CREATE_SQL_FILE, 'r') as f:
            data = f.read().replace('\n', '')
            table_create_queries = [_f for _f in data.split(";") if _f]

        for create_query in table_create_queries:
            cursor.execute(create_query)

    def _run_copy(self, cursor, base_table, s3_path):
        cursor.execute(COPY.format(base_table, s3_path, DP_IAM_ROLE_ARN))

    def _run_ctas(self, cursor, base_table, ddl_query):
        cursor.execute(ddl_query.format(base_table))

    def verify_ctas_table_content(self, cursor, base_table1):
        cursor.execute("select 2*count(*), 2*sum(col_smallint_raw), "
                       " 2*sum(col_smallint_bytedict) from {table};"
                       .format(table=base_table1))
        expected_result = cursor.fetchall()
        cursor.execute("SELECT count(*), sum(col_smallint_raw), "
                       " sum(col_smallint_bytedict) FROM ctas_table;")
        assert cursor.fetchall() == expected_result

    def test_burst_write_ctas_interleaved_with_copy(self, cluster, cursor, vector):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            self._setup_base_tables(cursor)
            base_table = self._get_random_all_shapes_tables()
            cursor.execute("SET query_group TO burst")
            self._run_copy(cursor, base_table, S3_PATH)
            self._start_and_wait_for_refresh(cluster)
            self._run_ctas(cursor, base_table, vector.DDL_CMD)
            self._check_last_query_bursted(cluster, cursor)
            self._run_copy(cursor, "ctas_table", S3_PATH)
            self.verify_ctas_table_content(cursor, base_table)
