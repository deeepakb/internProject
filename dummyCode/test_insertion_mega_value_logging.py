# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from raff.qp.qp_test import QPTest

DROP_IF_EXIST_TABLE = "DROP TABLE IF EXISTS {}"
CREATE_TABLE = """CREATE TABLE {}(a {} ENCODE {})
                  DISTSTYLE {}"""
DROP_TABLE = "DROP TABLE {}"
LOAD_INITIAL_TABLE = """ COPY large_super_object
                        FROM 's3://super-large-object/'
                        IAM_ROLE
                        'arn:aws:iam::467896856988:role/Redshift-S3-Write'"""
INSERT_MEGAVALUE = """INSERT INTO {}
                       (SELECT {} FROM large_super_object)"""
INSERT_NON_MEGAVALUE = """INSERT INTO {}
                       (SELECT {} FROM large_super_object
                        WHERE SIZE(a) < 1000000)"""
DOUBLE_VALUE_SIZE = """INSERT INTO large_super_object
                       (SELECT ARRAY_CONCAT(a,a) FROM large_super_object)"""
CHECK_MEGA_VALUE_INSERTED = """
                                SELECT inserted_mega_value = 't'
                                FROM stl_insert
                                WHERE query = pg_last_query_id()
                            """
ASSERT_MESSAGE = """(datatype: {}, encoding: {}, diststyle: {}, expect_mega_value: {})
                    result is not correct."""

GUCS = {
    'enable_variable_block_size': 'true',
    'enable_variable_block_for_super': 'true',
    'enable_variable_block_for_varbyte': 'true',
    'max_row_block_size': '64',
    'copy_max_record_size_mb': '64',
    'block_size': 1048576
}


def inserted_mega_value(res):
    for r in res:
        if r[0] is True:
            return True
    return False


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestInsertionMegaValueLogging(QPTest):
    """
        This test suite verifies stl_scan can correctly log whether a mega value
        is scanned from a perm table.
    """

    def _test_logging_result_as_expected(self, cursor):
        datatypes = ['super', 'varbyte(16777216)']
        encodings = ['raw', 'lzo', 'zstd']
        diststyles = ['ALL', 'EVEN']
        for datatype in datatypes:
            for encoding in encodings:
                for diststyle in diststyles:
                    for expect_mega_value in [True, False]:
                        dtype = datatype.split('(')[0]
                        table_name = "large_{}_object_{}_{}".format(
                            dtype, encoding, diststyle)
                        cursor.execute(DROP_IF_EXIST_TABLE.format(table_name))
                        cursor.execute(
                            CREATE_TABLE.format(table_name, datatype, encoding,
                                                diststyle))
                        insert_column = "a"
                        if dtype == 'varbyte':
                            insert_column = "json_serialize_to_varbyte(a)"
                        if expect_mega_value:
                            cursor.execute(
                                INSERT_MEGAVALUE.format(
                                    table_name, insert_column))
                        else:
                            cursor.execute(
                                INSERT_NON_MEGAVALUE.format(
                                    table_name, insert_column))
                        cursor.execute(CHECK_MEGA_VALUE_INSERTED)
                        res = cursor.fetchall()
                        assert inserted_mega_value(res) == expect_mega_value, \
                            ASSERT_MESSAGE.format(datatype, encoding, diststyle,
                                                  expect_mega_value)
                        cursor.execute(DROP_TABLE.format(table_name))

    def test_insertion_mega_value_logging(self, db_session, cluster,
                                          cluster_session):
        with cluster_session(
                gucs=GUCS, clean_db_before=True, clean_db_after=True):
            with self.db.cursor() as cursor:
                # Load source table, which has values with max size of 500 KB.
                cursor.execute("DROP TABLE IF EXISTS large_super_object")
                cursor.execute("CREATE TABLE large_super_object(a super)")
                cursor.execute(LOAD_INITIAL_TABLE)
                cursor.execute(DOUBLE_VALUE_SIZE)
                cursor.execute(DOUBLE_VALUE_SIZE)
                self._test_logging_result_as_expected(cursor)
