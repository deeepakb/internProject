# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
class TestBurstPushdown(BurstTest):
    '''This contains pushdown related tests for Burst.'''
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def test_block_str_ts_cast(self, db_session, cluster):
        '''Since Spectrum doesn't support the following timestamp fromats,
           we block string to timestamp cast push down to get the same
           result between Redshift and Burst.'''
        with db_session.cursor() as cursor:
            cursor.execute(
                'create table burst2120_ts (cvarchar varchar(30),'
                ' cchar char(30))'
            )
            cursor.execute(
                "insert into burst2120_ts values ('2/6/19 2:47', '2/19/19')"
            )

        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Run some queries to make sure that these casts return
        # expected results.
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(
                "select cvarchar from burst2120_ts where "
                "cvarchar::timestamp = '2019-02-06 02:47:00'"
            )
            assert cursor.fetchall() == [('2/6/19 2:47',)]

            cursor.execute(
                "select cvarchar from burst2120_ts where "
                "cvarchar::date = '2019-02-06'"
            )
            assert cursor.fetchall() == [('2/6/19 2:47',)]

            cursor.execute(
                "select cchar::timestamp = '2019-02-19 00:00:00' "
                "from burst2120_ts group by 1"
            )
            assert cursor.fetchall() == [(True,)]

            cursor.execute(
                "select cchar::date = '2019-02-19' "
                "from burst2120_ts group by 1"
            )
            assert cursor.fetchall() == [(True,)]
