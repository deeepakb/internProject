# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (setup_teardown_burst, BURST_DISABLE_C2S3_GUCS)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

CREATE_SQL = 'CREATE TABLE burst_439(c1 int, c2 int);'
POPULATE_SQL = 'INSERT INTO burst_439 VALUES (1, 1), (2, 1), (3, 2), (3, 2), (4, 3), (4, 3);'
UPDATE_SQL = 'UPDATE burst_439 SET c1 = 5 WHERE c2 = 2;'
DELETE_SQL = 'DELETE FROM burst_439 WHERE c1 = 2;'
INSERT_SQL = 'INSERT INTO burst_439 VALUES (2, 1), (2, 2)'
TEST_SQL = 'SELECT SUM(c1), SUM(c2), COUNT(*) FROM burst_439;'


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestVisibilityBurst(BurstTest):
    """
    Adds transaction visibility tests for Burst.
    """
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    # commit in auto-commit connection is not allowed in current jdbc driver
    @pytest.mark.no_jdbc
    def test_burst_visibility(self, cluster, db_session):
        # Create dataset.
        with db_session.cursor() as cursor:
            cursor.execute(CREATE_SQL)
            cursor.execute(POPULATE_SQL)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(TEST_SQL)
            assert cursor.fetchall() == [(17, 12, 6)]
        # Update dataset.
        with db_session.cursor() as cursor:
            cursor.execute(UPDATE_SQL)
        SNAPSHOT_IDENTIFIER_2 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_2)
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(TEST_SQL)
            assert cursor.fetchall() == [(21, 12, 6)]
        # Delete rows.
        with db_session.cursor() as cursor:
            cursor.execute(DELETE_SQL)
        SNAPSHOT_IDENTIFIER_3 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_3)
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(TEST_SQL)
            assert cursor.fetchall() == [(19, 11, 5)]
        # Insert rows.
        with db_session.cursor() as cursor:
            cursor.execute(INSERT_SQL)
        SNAPSHOT_IDENTIFIER_4 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_4)
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(TEST_SQL)
            assert cursor.fetchall() == [(23, 14, 7)]
