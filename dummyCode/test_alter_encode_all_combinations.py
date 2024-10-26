# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
from __future__ import print_function
import pytest
import os

from raff.common.db.db_exception import ProgrammingError
from raff.storage.alter_table_suite import AlterTableSuite
from raff.storage.alter_table_suite import enable_alter_column_type_gucs

__all__ = ["enable_alter_column_type_gucs"]

ALTER_ENCODING = """
    ALTER TABLE alter_encode_all_combinations
    ALTER COLUMN C_{data_type} ENCODE {encoding};
"""

ENCODING_TYPES = [
    "BYTEDICT",
    "DELTA",
    "DELTA32K",
    "LZO",
    "RUNLENGTH",
    "MOSTLY8",
    "MOSTLY16",
    "MOSTLY32",
    "AZ64",
    "TEXT255",
    "TEXT32K",
    "ZSTD",
    "RAW"
]

DATA_TYPES = [
    "smallint",
    "int",
    "bigint",
    "decimal",
    "real",
    "float8",
    "boolean",
    "char",
    "varchar",
    "date",
    "timestamp",
    "timestamptz",
]

@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.session_ctx(user_type='bootstrap')
class AlterEncodeAllCombinationsBase(AlterTableSuite):
    NAME = "alter_encode_alter_all_combinations"
    @property
    def name(self):
        return AlterEncodeAllCombinationsBase.NAME

    @property
    def testfiles_dir(self):
        return os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'testfiles')
    pass


"""
This class includes the test cases of running the alter column ENCODE command
against all data types for all encoding types. As different encodings are added
to data types this test will automatically run those commands.
"""

@pytest.mark.usefixtures("enable_alter_column_type_gucs")
class TestAlterEncodeAllCombinations(AlterEncodeAllCombinationsBase):
    """
    Run alter column encode for all encoding types for all data types.
    """
    def test_alter_encode_all_combinations(self, db_session):
        with db_session.cursor() as cursor:
            self.execute_test_file(
                "test_alter_encode_all_combinations_create_table",
                session=db_session)
            for data_type in DATA_TYPES:
                for encoding in ENCODING_TYPES:
                    print("ALTER ENCODING TO {} for {}".format(
                        encoding, data_type))
                    try:
                        cursor.execute(ALTER_ENCODING.format(
                            data_type=data_type, encoding=encoding))
                        self.execute_test_file(
                            "test_alter_encode_all_combinations_{}".format(
                                data_type),
                            session=db_session)
                    # This hits when a column at attempted to be altered to
                    # an encoding type that is not currently supported. For
                    # example:
                    # 'invalid encoding type specified for column "c_smallint"
                    except ProgrammingError:
                        pass

            print("Select all columns after all the ALTER commands have run")
            self.execute_test_file(
                "test_alter_encode_all_combinations_select_all",
                session=db_session)
