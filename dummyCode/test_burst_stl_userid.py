# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

QUERY_1 = 'SELECT * FROM web_sales LIMIT 10'
QUERY_ID = 'SELECT pg_last_query_id();'
VALIDATION = ('SELECT COUNT(distinct userid) '
              'FROM stl_query '
              'WHERE query IN (SELECT concurrency_scaling_query '
              'FROM stl_concurrency_scaling_query_mapping '
              'WHERE primary_query IN ({}, {}, {}))')


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstSTLUserId(BurstTest):
    """
    Test that the userid shown for Burst queries is not the Bootstrap user.
    """

    @contextmanager
    def burst_db_session(self, user_name, cluster):
        session_ctx = SessionContext(username=user_name)
        conn_params = cluster.get_conn_params()
        with DbSession(conn_params, session_ctx=session_ctx) as db_session:
            cursor = db_session.cursor()
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def test_stl_userid(self, cursor, db_session, cluster):
        USER_NAMES = ["fbnagel_00001", "fbnagel_00002", "fbnagel_00003"]
        QIDS = [None, None, None]

        # Run burst queries.
        i = 0
        while i < len(USER_NAMES):
            user_name = USER_NAMES[i]
            with self.burst_db_session(user_name, cluster) as cursor:
                cursor.execute(QUERY_1)
                cursor.execute(QUERY_ID)
                QIDS[i] = cursor.fetchall()[0][0]
                i += 1

        # Validate as Bootstap user.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(
                VALIDATION.format(QIDS[0], QIDS[1], QIDS[2]))
            assert bootstrap_cursor.fetchall() == [(3, )]
