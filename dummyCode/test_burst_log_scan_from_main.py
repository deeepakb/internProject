# Copyright 2021 Amazon.com, Inc. or its affiliates All Rights Reserved
import logging
import pytest
import random
import uuid

from datetime import date, timedelta
from raff.common.cluster.arcadia_configuration import enable_arcadia_gucs
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.host_type import HostType
from raff.monitoring.monitoring_test import MonitoringTestSuite, ClusterType
from raff.system_tests.suites.system_table_outside_cluster_helper import \
    SystemTableOutsideClusterHelper

log = logging.getLogger(__name__)


@pytest.mark.serial_only
class TestBurstLogScanFromMain(MonitoringTestSuite):

    user_id = 0
    query_id = 0
    burst_cluster_id = 0
    today_date = date.today()

    def _get_test_stls(self):
        stls = [
            # Pair of [STL base table, STL view]
            ['stl_query_detail', 'sys_query_detail'],
            ['stl_load_history_base', 'sys_load_history'],
            ['stl_unload_history_base', 'sys_unload_history'],
            ['stl_external_query_detail_base', 'sys_external_query_detail'],
            ['stl_user_load_error_detail', 'sys_load_error_detail'],
            ['stl_concurrency_scaling_query_mapping',
             'stl_concurrency_scaling_query_mapping']]
        return stls

    def _get_imm_cluster_id(self, cluster):
        if cluster.host_type == HostType.CLUSTER:
            return cluster.get_padb_conf_value('immutable_cluster_id')
        else:
            # All cluster has same imm cluster in simulate mode, so we
            # randomize the imm cluster to avoid STL file conflict on S3.
            return uuid.uuid4()

    def _get_guc_config(self, cluster):
        guc_config = {
            "enable_query_sys_table_log_from_S3": "true",
            "stl_enable_CN_fetching_LN_STL_from_s3": "true",
            "enable_scan_burst_log_from_s3": "true",
            "enable_stl_immutable_cluster_id_partition": "true",
            'multi_az_enabled': "false",
        }
        guc_config["immutable_cluster_id"] = self._get_imm_cluster_id(cluster)
        guc_config["s3_stl_bucket"] = self.get_s3_stl_bucket_guc_value(cluster)
        guc_config["cluster_id"] = cluster.get_padb_conf_value('cluster_id')

        self.user_id = 1000000 + random.randint(0, 1000000)
        self.query_id = 100000001 + random.randint(0, 1000000)
        self.burst_cluster_id = random.randint(0, 1000000)
        return guc_config

    def _prepare_stl_data(self, cluster, guc_config, stls, user_id, query_id):
        '''
        Prepare burst STL on S3.
        '''
        stl_contents = self.generate_sample_stl_content(user_id, query_id)
        for [stl_base, stl_view] in stls:
            # Clean recent 7 days STL on S3.
            history_date = []
            for i in range(7):
                date_str = (self.today_date - timedelta(days=i)).\
                    strftime("%Y%m%d")
                history_date.append(date_str)
            MonitoringTestSuite.clean_stl_log_files_on_s3_for_dates(
                cluster, MonitoringTestSuite.get_s3_client(cluster),
                guc_config, stl_base, ClusterType.Burst, history_date)
            # Generate and upload burst STL data to S3.
            stl_content = stl_contents[stl_base]
            SystemTableOutsideClusterHelper.generate_stl_file_and_upload_to_s3(
                cluster, guc_config, ClusterType.Burst, stl_base, stl_content,
                log_range_days=1, db_instance_id=self.burst_cluster_id)

    def _verify_scan_burst_stl(self, cluster, cluster_session, guc_config,
                               stls, user_id, expected_qid):
        with cluster_session(gucs=guc_config, clean_db_before=True,
                             clean_db_after=True):
            ctx = SessionContext(user_type=SessionContext.BOOTSTRAP)
            conn_params = cluster.get_conn_params()
            with DbSession(conn_params, session_ctx=ctx) as db_session:
                query_template = "select {} from {} where {} = {};"
                for [stl_base, stl_view] in stls:
                    if stl_view == 'stl_concurrency_scaling_query_mapping':
                        query = query_template.format(
                            'primary_query', stl_view, 'userid', user_id)
                    else:
                        query = query_template.format(
                            'query_id', stl_view, 'user_id', user_id)
                    res = self.run_query(db_session, cluster, query)
                    if expected_qid is None:
                        assert [] == res
                    else:
                        log.info('view:{}'.format(stl_view))
                        assert 1 == len(res), \
                            'failed: {}'.format(stl_view)
                        assert expected_qid == int(res[0][0]),\
                            'failed: {}'.format(stl_view)

    def test_scan_burst_stl_on_serverless_cluster(
            self, cluster, cluster_session):
        '''
        This test generate Serverless STL file and upload file to S3 in burst
        cluster's partition, then verify that the STL query should be able to
        query back the burst STL data from S3 in serverless.
        '''
        stls = self._get_test_stls()
        guc_config = self._get_guc_config(cluster)
        guc_config.update(enable_arcadia_gucs(cluster))
        guc_config['scan_burst_stl_from_s3_enabled_stls'] = ''
        self._prepare_stl_data(
            cluster, guc_config, stls, self.user_id, self.query_id)
        self._verify_scan_burst_stl(
            cluster, cluster_session, guc_config, stls, self.user_id,
            self.query_id)

    def test_enable_scan_burst_stl_on_provisioned_cluster(
            self, cluster, cluster_session):
        '''
        This test generate Serverless STL file and upload file to S3 in burst
        cluster's partition, then verify that the STL query should be able to
        query back the burst STL data from S3 in provisioned cluster when
        <scan_burst_stl_from_s3_enabled_stls> specified the STL base table.
        '''
        stls = self._get_test_stls()
        guc_config = self._get_guc_config(cluster)
        guc_config['is_arcadia_cluster'] = 'false'
        guc_config['enable_arcadia'] = 'false'
        # Enable scan burst stl on provisioned mode.
        guc_config['scan_burst_stl_from_s3_enabled_stls'] = \
            ('stll_query_detail,stll_load_history_base,'
             'stll_unload_history_base,stll_external_query_detail_base,'
             'stll_user_load_error_detail,'
             'stll_concurrency_scaling_query_mapping')

        self._prepare_stl_data(
            cluster, guc_config, stls, self.user_id, self.query_id)
        self._verify_scan_burst_stl(
            cluster, cluster_session, guc_config, stls, self.user_id,
            self.query_id)

    def test_disable_scan_burst_stl_on_provisioned_cluster(
            self, cluster, cluster_session):
        '''
        This test generate Serverless STL file and upload file to S3 in burst
        cluster's partition, then verify that the STL query is not able to
        query back the burst STL data from S3 in provisioned mode when
        <scan_burst_stl_from_s3_enabled_stls> is empty.
        '''
        stls = self._get_test_stls()
        guc_config = self._get_guc_config(cluster)
        # Disable scan burst stl on provisioned mode.
        guc_config['is_arcadia_cluster'] = 'false'
        guc_config['enable_arcadia'] = 'false'
        guc_config['scan_burst_stl_from_s3_enabled_stls'] = ''
        guc_config['enable_scan_burst_log_from_s3'] = 'false'

        self._prepare_stl_data(
            cluster, guc_config, stls, self.user_id, self.query_id)
        self._verify_scan_burst_stl(
            cluster, cluster_session, guc_config, stls, self.user_id,
            expected_qid=None)
