# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import uuid
import pytest

from raff.burst.burst_test import BurstTest
from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.common.db.session_context import SessionContext

log = logging.getLogger(__name__)

# Create a table with one dummy column.
SQL_CREATE_TABLE = "create table t1(a int) diststyle even;"
# Insert values to the table created above
SQL_BASIC = "insert into t1 values ({});"

GUCS = dict(
    is_burst_cluster='true',
    s3commit_tossing_mode='0',
    enable_backups_in_simulated='false',
    burst_mode='1',
    s3_stl_bucket='padb-monitoring-test',
    query_id_tracking_mode='2',
    query_id_reservation_batch_size='10',
    immutable_cluster_id=str(uuid.uuid4()),
    enable_mirror_to_s3='0',
    enable_commits_to_dynamo='0'
)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.session_ctx(user_type=SessionContext.REGULAR)
class TestBurstQueryId(BurstTest, MonitoringTestSuite):
    def test_burst_cluster_exhausted_query_id(
            self, cluster, db, cluster_session, db_session):
        """
        This test validates that the burst cluster will see event trace
        after query ids are exhausted.
        """
        query = """
            SELECT COUNT(*) FROM stl_event_trace
            WHERE event_name = 'EtMonitoringTracing' AND eventtime > '{}'
            AND message like '%Query id exhausted in burst cluster%';
        """
        updated_gucs = GUCS
        updated_gucs['gconf_event'] = \
            cluster.get_padb_conf_value('gconf_event') + \
            '|EtMonitoringTracing,level=ElError'
        with cluster_session(
                gucs=updated_gucs,
                clean_db_before=True,
                clean_db_after=True):
            cluster.reboot_cluster()
            query_count = int(GUCS['query_id_reservation_batch_size'])
            log.info("gucs = " + str(GUCS))
            with db_session.cursor() as cursor:
                cursor.execute("SELECT NOW();")
                start_time = cursor.fetch_scalar()
                cursor.execute(SQL_CREATE_TABLE)
                for i in range(query_count):
                    burst_query = SQL_BASIC.format(i)
                    log.info('burst_query = {}'.format(burst_query))
                    cursor.execute(burst_query)

                # Verify the query id exhaustion is detected.
                verification_query = query.format(start_time)
                log.info('verification_query = {}'.format(verification_query))
                record_count = self.run_query(
                    db, cluster, verification_query)
                log.info('record_count = {}'.format(record_count))
                assert record_count[0][0] > 0, \
                    'Query id exhaustion is not detected'
            db_session.close()
