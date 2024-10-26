# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import uuid
import datetime

import pytest
from raff.burst.burst_test import BurstTest
from raff.arcadia.arcadia_test import (ArcadiaTest, BASIC_ARCADIA_GUCS,
                                       ARCADIA_RESERVATION_BILLING_MODEL_GUCS)

log = logging.getLogger(__name__)

BASE_MULTI_AZ_GUCS = {
    'burst_mode': '1',
    'multi_az_enabled': 'true',
    'is_multi_az_primary': 'true',
    'selective_dispatch_level': '0',
    'enable_result_cache': 'false',
    'enable_sqa_by_default': 'false',
    'enable_arcadia_system_views_in_provisioned_mode': 'true',
    'try_multi_az_first': 'true',
    'wlm_json_configuration':
    '[{"concurrency_scaling":"auto", "auto_wlm": true}]'
}

ARCADIA_MAZ_GUCS = dict(BASE_MULTI_AZ_GUCS, **{
    'multi_az_disable_burst_billing': 'false'
})

ARCADIA_MAZ_ORIGINAL_MODEL_BILLING_GUCS = dict(ARCADIA_MAZ_GUCS,
                                               **BASIC_ARCADIA_GUCS)
ARCADIA_MAZ_RESERVATION_MODEL_BILLING_GUCS = dict(
    ARCADIA_MAZ_GUCS, **ARCADIA_RESERVATION_BILLING_MODEL_GUCS)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.no_jdbc
@pytest.mark.arcadia_billing
@pytest.mark.encrypted_only
class TestMultiAzArcadiaBilling(BurstTest, ArcadiaTest):
    @pytest.mark.arcadia_billing_reservation_model
    def test_maz_arcadia_billing_reservation_model(
            self, cluster, cluster_session, db_session):
        '''
        Incorporates Serverless Reservation Billing Model only.
        Test to verify that queries run on MultiAZ secondary clusters have
        usage logged in the burst usage system table, but generates usage
        RIS reports with "is_burst" flag set to false.
        '''
        with cluster_session(gucs=ARCADIA_MAZ_RESERVATION_MODEL_BILLING_GUCS):
            with self.db.cursor() as cursor:
                test_start_time = self.get_current_timestamp(cursor)

            with db_session.cursor() as cursor:
                cursor.execute("create table t1 (col1 int)")
                cursor.execute("insert into t1 values(1)")

            SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_first',
                                                  str(uuid.uuid4().hex)))

            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
            cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")

            with db_session.cursor() as cursor:
                cursor.execute("select now();")
                start_time = cursor.fetch_scalar()
                cursor.execute("select * from t1")
                cursor.fetchall()
                qid = self.last_query_id(cursor)
                self.check_last_query_ran_on_multi_az_with_qid(
                    cluster, qid, is_arcadia=True)
                # check if compute type of sys_query_history is secondary
                query_text = ("SELECT count(*) "
                              "FROM sys_query_history "
                              "WHERE query_text = 'select * from t1;' "
                              "AND compute_type = 'secondary' "
                              "AND start_time > '{}' ".format(start_time))
                cursor.execute(query_text)
                query_result = cursor.fetch_scalar()
                assert query_result == 1, \
                    'There should be only one record with ' \
                    'compute_type == secondary'

                # Verify multiaz query is billed.
                bill_time = self.bill_time_ms(
                    cluster, test_start_time, xid=-1, qid=qid, is_burst=True)
                assert bill_time > 0

                # MAZ secondary RIS reports would have is_burst set to False.
                # Verify there are no burst RIS reports created.
                ris_burst_reports_usage_list = self.get_ris_reports_usage_list(
                    cluster, test_start_time, is_burst=True)
                num_burst_ris_reports = len(ris_burst_reports_usage_list)
                assert num_burst_ris_reports == 0

                # Verify that we have non-burst RIS reports created.
                ris_reports_usage_list = self.get_ris_reports_usage_list(
                    cluster, test_start_time, is_burst=False)
                num_ris_reports = len(ris_reports_usage_list)
                assert num_ris_reports > 0

    def test_maz_arcadia_billing_original_model(self, cluster, cluster_session,
                                                db_session):
        '''
        Incorporates Original Serverless Billing Model only.
        Test to verify that queries run on MultiAZ secondary clusters have
        usage logged in the burst usage system table, but generates usage
        RIS reports with "is_burst" flag set to false.
        '''
        with cluster_session(gucs=ARCADIA_MAZ_ORIGINAL_MODEL_BILLING_GUCS):
            with self.db.cursor() as cursor:
                test_start_time = self.get_current_timestamp(cursor)

            with db_session.cursor() as cursor:
                cursor.execute("create table t1 (col1 int)")
                cursor.execute("insert into t1 values(1)")

            SNAPSHOT_IDENTIFIER = ("{}-{}".format('test_multi_az_first',
                                                  str(uuid.uuid4().hex)))

            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
            cluster.run_xpx("multi_az_acquire_secondary arn 1.1.1.1 psk")

            with db_session.cursor() as cursor:
                cursor.execute("select now();")
                start_time = cursor.fetch_scalar()
                cursor.execute("select * from t1")
                cursor.fetchall()
                qid = self.last_query_id(cursor)
                self.check_last_query_ran_on_multi_az_with_qid(
                    cluster, qid, is_arcadia=True)
                # check if compute type of sys_query_history is secondary
                query_text = ("SELECT count(*) "
                              "FROM sys_query_history "
                              "WHERE query_text = 'select * from t1;' "
                              "AND compute_type = 'secondary' "
                              "AND start_time > '{}' ".format(start_time))
                cursor.execute(query_text)
                query_result = cursor.fetch_scalar()
                assert query_result == 1, \
                    'There should be only one record with ' \
                    'compute_type == secondary'

                # Verify multiaz query is billed.
                bill_time = self.bill_time_ms(
                    cluster, test_start_time, xid=-1, qid=qid, is_burst=True)
                assert bill_time > 0

                # MAZ secondary RIS reports would have is_burst set to False.
                # Verify there are no burst RIS reports created.
                ris_burst_reports_usage_list = self.get_ris_reports_usage_list(
                    cluster, test_start_time, is_burst=True)
                num_burst_ris_reports = len(ris_burst_reports_usage_list)
                assert num_burst_ris_reports == 0

                # Verify that we have non-burst RIS reports created.
                ris_reports_usage_list = self.get_ris_reports_usage_list(
                    cluster, test_start_time, is_burst=False)
                num_ris_reports = len(ris_reports_usage_list)
                assert num_ris_reports > 0
