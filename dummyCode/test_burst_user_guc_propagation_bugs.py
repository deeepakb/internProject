# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstUserGucPropagationSS(BurstWriteTest):
    def test_timezone_with_alter_user(self, cluster, db_session):
        db_session._establish_session_resources()
        reg_user_name = db_session.session_ctx.username
        super_ctx = SessionContext(user_type='super')
        super_session = DbSession(cluster.get_conn_params(),
                                  session_ctx=super_ctx)
        with super_session.cursor() as cursor:
            cursor.execute("ALTER USER {} SET timezone TO 'Asia/Seoul'".format(
                reg_user_name))

        with db_session.cursor() as cursor:
            cursor.execute("create table t(a timestamptz) diststyle even;")
            cursor.execute("insert into t values('2023-01-01 UTC');")
            cursor.execute("select to_char(a, 'yyyy-mm-dd hh24') from t;")
            res_main = cursor.fetchone()[0]
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute("select to_char(a, 'yyyy-mm-dd hh24') from t;")
            res_burst = cursor.fetchone()[0]
            self._check_last_query_bursted(cluster, cursor)
            assert res_main == res_burst
            assert res_main == "2023-01-01 09"
