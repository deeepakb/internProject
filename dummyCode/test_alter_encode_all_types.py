# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import os
from copy import deepcopy

from raff.common.dimensions import Dimensions
from raff.storage.alter_table_suite import AlterTableSuite
from raff.storage.alter_table_suite import enable_alter_column_type_gucs

__all__ = ["enable_alter_column_type_gucs"]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.session_ctx(user_type='bootstrap')
class AlterColumnEncodeBase(AlterTableSuite):
    NAME = "alter_column_encode"
    @property
    def name(self):
        return AlterColumnEncodeBase.NAME

    @property
    def testfiles_dir(self):
        return os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'testfiles')
    pass


@pytest.mark.usefixtures("enable_alter_column_type_gucs")
class TestAlterColumnEncodeAllTypes(AlterColumnEncodeBase):
    """
    Run alter column encode with all compatitble encode types.
    """
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "col_encode": [
            # SMALLINT
            "c0 ENCODE RAW", "c0 ENCODE AZ64", "c0 ENCODE BYTEDICT",
            "c0 ENCODE DELTA", "c0 ENCODE LZO", "c0 ENCODE MOSTLY8",
            "c0 ENCODE RUNLENGTH",
            # INTERGET
            "c1 ENCODE RAW", "c1 ENCODE AZ64", "c1 ENCODE BYTEDICT",
            "c1 ENCODE DELTA", "c1 ENCODE DELTA32K", "c1 ENCODE LZO",
            "c1 ENCODE MOSTLY8", "c1 ENCODE MOSTLY16", "c1 ENCODE RUNLENGTH",
            # BIGINT
            "c2 ENCODE RAW", "c2 ENCODE AZ64", "c2 ENCODE BYTEDICT",
            "c2 ENCODE DELTA", "c2 ENCODE DELTA32K", "c2 ENCODE LZO",
            "c2 ENCODE MOSTLY8", "c2 ENCODE MOSTLY16", "c2 ENCODE MOSTLY32",
            "c2 ENCODE RUNLENGTH",
            # DECIMAL
            "c3 ENCODE RAW", "c3 ENCODE AZ64", "c3 ENCODE BYTEDICT",
            "c3 ENCODE DELTA", "c3 ENCODE DELTA32K", "c3 ENCODE LZO",
            "c3 ENCODE MOSTLY8", "c3 ENCODE MOSTLY16", "c3 ENCODE MOSTLY32",
            "c3 ENCODE RUNLENGTH",
            # FLOAT4
            "c4 ENCODE RAW", "c4 ENCODE BYTEDICT", "c4 ENCODE RUNLENGTH",
            # FLOAT8
            "c5 ENCODE RAW", "c5 ENCODE BYTEDICT", "c5 ENCODE RUNLENGTH",
            # BOOL
            "c6 ENCODE RAW", "c6 ENCODE RUNLENGTH",
            # CHAR
            "c7 ENCODE RAW", "c7 ENCODE BYTEDICT", "c7 ENCODE LZO",
            "c7 ENCODE RUNLENGTH",
            # VARCHAR
            "c8 ENCODE RAW", "c8 ENCODE BYTEDICT", "c8 ENCODE LZO",
            "c8 ENCODE RUNLENGTH", "c8 ENCODE TEXT255",
            "c8 ENCODE TEXT32K",
            # DATE
            "c9 ENCODE RAW", "c9 ENCODE AZ64", "c9 ENCODE BYTEDICT",
            "c9 ENCODE DELTA", "c9 ENCODE DELTA32K", "c9 ENCODE LZO",
            "c9 ENCODE RUNLENGTH",
            # TIMESTAMP
            "c10 ENCODE RAW", "c10 ENCODE AZ64", "c10 ENCODE BYTEDICT",
            "c10 ENCODE DELTA", "c10 ENCODE DELTA32K", "c10 ENCODE LZO",
            "c10 ENCODE RUNLENGTH",
            # TIMESTAMPZ
            "c11 ENCODE RAW", "c11 ENCODE AZ64", "c11 ENCODE BYTEDICT",
            "c11 ENCODE DELTA", "c11 ENCODE DELTA32K", "c11 ENCODE LZO",
            "c11 ENCODE RUNLENGTH",
            ]
        })

    def _transform_test(self, test, vector):
        transformed_test = deepcopy(test)
        transformed_test.query.sql = test.query.sql.format(
            col_encode=vector.col_encode)
        return transformed_test

    def test_alter_encode_all_types(self, db_session, vector):
        self.execute_test_file(
            "test_alter_encode_all_types", session=db_session, vector=vector)
