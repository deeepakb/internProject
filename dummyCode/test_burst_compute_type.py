# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import uuid

from contextlib import contextmanager
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

CUSTOM_AUTO_GUCS = {
    'is_arcadia_cluster': 'false',
    'enable_arcadia': 'false',
    'enable_arcadia_system_views_in_provisioned_mode': 'true',
    'enable_mirror_to_s3': '0',
    'enable_commits_to_dynamo': '0'
}


@pytest.mark.localhost_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS, is_arcadia=False,
                               initdb_before=True, initdb_after=True)
@pytest.mark.serial_only
class TestBurstComputeType():
    """
    Test that Burst queries have value of burst compute type.
    """

    @contextmanager
    def burst_db_session(self, db_session):
        cursor = db_session.cursor()
        cursor.execute("set query_group to burst")
        yield cursor
        cursor.execute("reset query_group")

    @pytest.mark.usefixtures("setup_teardown_burst")
    def test_burst_compute_type(self, db_session, cluster, cluster_session):
        test_gucs = {
            "s3_backup_key_prefix": 'connector/{}/12345'.format(
                str(uuid.uuid4().hex[:8]))
        }
        test_table = 'test_tab_' + str(uuid.uuid4())[:8]
        with self.burst_db_session(db_session) as cursor:
            cursor.execute("create table {} (col1 int)".format(test_table))
            cursor.execute("insert into {} values(1)".format(test_table))
        with cluster_session(gucs=test_gucs):
            # Trigger a backup since latest backup info is not persisted
            # across restarts
            backup_id = ("{}-{}".format(cluster.cluster_identifier,
                                        str(uuid.uuid4().hex)))
            cluster.backup_cluster(backup_id)

            with self.burst_db_session(db_session) as cursor:
                try:
                    cursor.execute("select now();")
                    test_start_time = cursor.fetch_scalar()
                    cursor.execute("select * from {}".format(test_table))
                    query_text = ("SELECT trim(compute_type) as compute_type "
                                  "FROM sys_query_history "
                                  "WHERE query_text LIKE 'select * from {}%' AND "
                                  "query_label = 'burst' AND status = 'success' "
                                  "AND start_time > '{}';"
                                  ).format(test_table, test_start_time)
                    cursor.execute(query_text)
                    query_result = cursor.fetch_scalar()
                    assert query_result == 'primary-scaled', \
                        'compute_type should be primary-scaled in burst cluster'
                finally:
                    drop_table = "drop table if exists {};".format(test_table)
                    cursor.execute(drop_table)
