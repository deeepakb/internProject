# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import os

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


class BurstIdentityUniqBase(BurstWriteTest):
    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    def base_default_identity_insert_update_uniqueness(self, cluster):
        """
        Tests uniqueness of auto-generated values in a default identity column
        before and after INSERT and UPDATE override the value. The test does
        the following and verifies the count of distinct values at each step:
        1. Insert default values into t1 using identity(1,1).
        2. Insert default values into t2 using identity(1,2).
        3. Create table t3 for insert into select tests.
        4. Insert default values during insert into select.
        5. Update to default value.
        6. Insert overriden values (100000, 200000 and 300000) and check max
        value.
        7. Insert overriden values during insert into select.
        8. Update to overriden value (4000) and check max value (4000).
        9. Repeat steps 1, 2 and 4, and check max value (should be 4000).
        10. Repeat step 5 and check max value (should be back to 3000) since we
            update the 4000 value with the default.

        The fact that auto-generated values after the override does not change
        the max value proves that we do not take into account the customer
        provided values when maintaining the max identity values.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        self.execute_test_file(
            'default_identity_insert_update_uniqueness_set_up',
            session=db_session,
            query_group='metrics')
        self._start_and_wait_for_refresh(cluster)
        self.execute_test_file(
            'default_identity_insert_update_uniqueness_validation',
            session=db_session,
            query_group='burst')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '5',
        'burst_enable_write': 'true',
        'burst_enable_write_id_col': 'true'
    })
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
class TestBurstIdentityUniqSSmode(BurstIdentityUniqBase):
    def test_burst_default_identity_insert_update_uniqueness(self, cluster):
        self.base_default_identity_insert_update_uniqueness(cluster)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_enable_write', 'true'), ('burst_enable_write_id_col', 'true')]))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_id_col': 'true'})
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstIdentityUniqCluster(BurstIdentityUniqBase):
    def test_burst_default_identity_insert_update_uniqueness(self, cluster):
        self.base_default_identity_insert_update_uniqueness(cluster)
