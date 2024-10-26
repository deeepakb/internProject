# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]


@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.localhost_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestBurstUniqueOnExpr(BurstTest):
    """
    [DP-19551] Fix unique on expression wrong result
    """
    @property
    def generate_sql_res_files(self):
        return True

    def test_burst_unique_on_expr(self, db_session, cluster_session):
        custom_gucs = {"auto_analyze": "false"}
        with cluster_session(gucs=custom_gucs):
            self.execute_test_file(
                'test_burst_unique_on_expr', session=db_session)
