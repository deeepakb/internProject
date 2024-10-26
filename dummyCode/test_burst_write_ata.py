# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_CMD = "insert into {}.{} values (1,1), (2,2), (3,3), (4,4), (5,5)"
INSERT_SELECT_CMD = "insert into {schema}.{tbl} select * from {schema}.{tbl}"
DELETE_CMD = "delete from {}.{} where c0 < 2"
UPDATE_CMD = "update {}.{} set c0 = c0 + 2 where c1 > 3"
S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_CMD = ("COPY {}.{} "
            "FROM "
            "'{}' "
            "DELIMITER ',' "
            "CREDENTIALS "
            "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteATA(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c0)'],
                sortkey=['sortkey(c0)'],
                dml_type=['insert', 'delete', 'update', 'copy']))

    def _setup_tables(self, cursor, schema, source_table, target_table,
                      diststyle, sortkey):

        cursor.execute("create table {}.{}(c0 int, c1 int) {} {}".format(
            schema, source_table, diststyle, sortkey))
        cursor.execute("create table {}.{}(c0 int, c1 int) {} {}".format(
            schema, target_table, diststyle, sortkey))
        cursor.execute(INSERT_CMD.format(schema, source_table))
        cursor.execute(INSERT_CMD.format(schema, target_table))
        for i in range(5):
            cursor.execute(
                INSERT_SELECT_CMD.format(schema=schema, tbl=source_table))
            cursor.execute(
                INSERT_SELECT_CMD.format(schema=schema, tbl=target_table))

    def _generate_dml_cmd(self, dml_type, schema, table):
        if dml_type == 'insert':
            return INSERT_CMD.format(schema, table)
        elif dml_type == 'delete':
            return DELETE_CMD.format(schema, table)
        elif dml_type == 'update':
            return UPDATE_CMD.format(schema, table)
        else:
            return COPY_CMD.format(schema, table, S3_PATH)

    def test_burst_write_ata_1(self, cluster, vector):
        """
        This test first bursted write query on both source table
        and target table, then conduct ATA. Check after ATA,
        write query on both source table and target table are not bursted.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema

        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            target_table = "test_bw_target"
            source_table = "test_bw_source"
            self._setup_tables(cursor, schema, source_table, target_table,
                               vector.diststyle, vector.sortkey)

            self._start_and_wait_for_refresh(cluster)

            dml_cmd_source = self._generate_dml_cmd(vector.dml_type, schema,
                                                    source_table)
            dml_cmd_target = self._generate_dml_cmd(vector.dml_type, schema,
                                                    target_table)
            # burst write on source_table
            cursor.execute(dml_cmd_source)
            self._check_last_query_bursted(cluster, cursor)
            # burst write on target table
            cursor.execute(dml_cmd_target)
            self._check_last_query_bursted(cluster, cursor)

            # conduct ATA
            log.info("begin to conduct ata")
            cursor.execute("alter table {} append from {}".format(
                target_table, source_table))

            self._check_ownership_state(schema, target_table, None)
            self._check_ownership_state(schema, source_table, None)
            log.info("begin write query after ata")
            # write query one more time after ddl, should not burst
            cursor.execute(dml_cmd_source)
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(dml_cmd_target)
            self._check_last_query_didnt_burst(cluster, cursor)
            # validate
            self._validate_table(cluster, schema, source_table, vector.diststyle)
            self._validate_table(cluster, schema, target_table, vector.diststyle)


    def test_burst_write_ata_2(self, cluster, vector):
        """
        This test covers the following sequence of cmds.
            backup + refresh
            ata
            dml <--- no burst
            backup + refresh
            dml <---- burst
        Validate each step is correct.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema

        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            target_table = "test_bw_target"
            source_table = "test_bw_source"
            self._setup_tables(cursor, schema, source_table, target_table,
                               vector.diststyle, vector.sortkey)

            self._start_and_wait_for_refresh(cluster)
            # conduct ATA
            log.info("begin to conduct ata")
            cursor.execute("alter table {} append from {}".format(
                target_table, source_table))
            self._check_ownership_state(schema, target_table, None)
            self._check_ownership_state(schema, source_table, None)

            log.info("begin write query after ata")
            dml_cmd_source = self._generate_dml_cmd(vector.dml_type, schema,
                                                    source_table)
            dml_cmd_target = self._generate_dml_cmd(vector.dml_type, schema,
                                                    target_table)
            # write query after ata should not burst
            cursor.execute(dml_cmd_source)
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(dml_cmd_target)
            self._check_last_query_didnt_burst(cluster, cursor)
            # validate
            self._validate_table(cluster, schema, source_table, vector.diststyle)
            self._validate_table(cluster, schema, target_table, vector.diststyle)

            # take another backup and refresh, write query could be bursted
            self._start_and_wait_for_refresh(cluster)
            # burst write on source_table
            cursor.execute(dml_cmd_source)
            self._check_last_query_bursted(cluster, cursor)
            # burst write on target table
            cursor.execute(dml_cmd_target)
            self._check_last_query_bursted(cluster, cursor)
            # validate
            self._validate_table(cluster, schema, source_table, vector.diststyle)
            self._validate_table(cluster, schema, target_table, vector.diststyle)
