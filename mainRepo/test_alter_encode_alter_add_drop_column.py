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
class AlterEncodeAlterAddDropColBase(AlterTableSuite):
    NAME = "alter_encode_alter_add_drop_column"
    @property
    def name(self):
        return AlterEncodeAlterAddDropColBase.NAME

    @property
    def testfiles_dir(self):
        return os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'testfiles')
    pass


"""
This class includes the test cases of running alter column encode command
without concurrent txn on target table. Following test cases are included:
1. run "alter column encode" within transaction block and
   without concurrent DML.
"""


@pytest.mark.usefixtures("enable_alter_column_type_gucs")
class TestAlterEncodeAlterAddDropCol(AlterEncodeAlterAddDropColBase):
    """
    Run alter column encode with transaction block.
    Following test cases are included:
    """
    def test_alter_encode_alter_add_col(self, db_session):
        self.execute_test_file("test_alter_encode_alter_add_col",
                               session=db_session)

    def test_alter_encode_alter_drop_col(self, db_session):
        self.execute_test_file("test_alter_encode_alter_drop_col",
                               session=db_session)

    def test_alter_encode_alter_add_drop_col(self, db_session):
        self.execute_test_file("test_alter_encode_alter_add_drop_col",
                               session=db_session)

    def test_alter_encode_alter_distkey_add_drop_col(self, db_session):
        self.execute_test_file("test_alter_encode_alter_distkey_add_drop_col",
                               session=db_session)
