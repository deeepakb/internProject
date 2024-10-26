# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from contextlib import contextmanager

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_status import BurstStatus
from raff.common.db.session import DbSession
from raff.common.cred_helper import get_key_auth_str
from raff.common.profile import Profiles
from raff.burst.burst_temp_write import BurstTempWrite, burst_user_temp_support_gucs

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


class BurstCopyDefaultValueColGUCBase(BurstTempWrite):
    @contextmanager
    def create_range_table(self, cursor, is_temp):
        table_type = 'temp' if is_temp else ''
        try:
            query = """create {} table if not exists RangeTableNumber (
                            id integer,
                            year integer,
                            name varchar(64),
                            rating varchar(64),
                            default_c0 timestamp DEFAULT getdate(),
                            default_c1 int DEFAULT 10,
                            default_c2 varchar(max) DEFAULT '100'
                        )
                        diststyle even"""
            cursor.execute(query.format(table_type))
            yield
        finally:
            cursor.execute("drop table RangeTableNumber")

    def base_test_burst_copy_default_value_column(self, cluster, guc_value,
                                                  is_temp):
        credential = get_key_auth_str(profile=Profiles.COOKIE_CORE)
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Large test. copy table with large rows.
        with db_session.cursor() as cursor:
            with self.create_range_table(cursor, is_temp):
                self._start_and_wait_for_refresh(cluster)
                cursor.execute("set query_group to burst;")
                cursor.run_copy(
                    "RangeTableNumber",
                    "dynamodb://RangeTableNumber",
                    credential,
                    readratio=100,
                    COMPUPDATE="OFF")
                if guc_value == 'true':
                    self._check_last_copy_bursted(cluster, cursor)
                else:
                    self._check_last_copy_didnt_burst_with_code(
                        cluster, cursor,
                        BurstStatus.burst_copy_on_tbl_with_default_value_col)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true'),
               ('burst_enable_write_copy_default_value_column', 'false')]))
@pytest.mark.custom_local_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true'),
               ('burst_enable_write_copy_default_value_column', 'false')]))
class TestBurstCopyDefaultValueColumnGUCOff(BurstCopyDefaultValueColGUCBase):
    def test_burst_copy_default_value_column_off(self, cluster, is_temp):
        """
        Test: burst copy on table with default value column should not succeed
              when GUC is set to false.
        """
        self.base_test_burst_copy_default_value_column(cluster, 'false', is_temp)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true'),
               ('burst_enable_write_copy_default_value_column', 'true')]))
@pytest.mark.custom_local_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true'),
               ('burst_enable_write_copy_default_value_column', 'true')]))
class TestBurstCopyDefaultValueColumnGUCOn(BurstCopyDefaultValueColGUCBase):
    def test_burst_copy_default_value_column_on(self, cluster, is_temp):
        """
        Test: burst copy on table with default value column should not succeed
              when GUC is set to true.
        """
        self.base_test_burst_copy_default_value_column(cluster, 'true', is_temp)
