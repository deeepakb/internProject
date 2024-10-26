# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import logging

from test_burst_mv_refresh import TestBurstWriteMVBase
from raff.common.db.redshift_db import RedshiftDb
from raff.datasharing.datasharing_test import (
    DatasharingBurstTest, consumer_cluster, datashare_context, datashare_setup,
    setup_teardown_burst_datasharing, customise_burst_cluster_datasharing)
from raff.burst.burst_test import setup_teardown_burst
from raff.common.node_type import NodeType
from raff.burst.burst_write import burst_write_basic_gucs

__all__ = ["setup_teardown_burst"]
log = logging.getLogger(__name__)

# Point to existing consumer cluster.
# Remove if we have a preferered runnable.
# pytestmark = pytest.mark.consumer_cluster_existing(
#    existing_identifier='nsanketh-test-a107')

pytestmark = pytest.mark.consumer_cluster_kwargs(
    NumberOfNodes=3, NodeType=NodeType.I3EN_4XLARGE, Encrypted=True)

CONSUMER_CLUSTER_GUCS = dict(
    enable_burst_datasharing='true',
    enable_burst_datasharing_volt_tt='true',
    enable_burst_async_acquire='false',
    enable_burst_write_for_datasharing='false')
CONSUMER_CLUSTER_GUCS.update(burst_write_basic_gucs)

BURST_CLUSTER_GUCS = dict(
    enable_burst_datasharing='true',
    enable_burst_datasharing_volt_tt='true',
    enable_data_sharing_result_cache='false',
    diff_topologies_mode='2',
    enable_redcat_table_integration='true',
    enable_redshift_federation='true',
    use_s3commit_in_rslocal_federation='true')

INSERT_DS_SQL = '''
                insert into test_burst_tbl select sm_ship_mode_sk from
                ds_test_schema.ship_mode order by 1 limit 1
                '''

DS_SQL = '''
            select sm_ship_mode_sk from
            ds_test_schema.ship_mode order by 1 limit 1
        '''


def run_query(cluster, db, query, user=None):
    with RedshiftDb(cluster.get_conn_params(db_name=db)) as conn:
        with conn.cursor() as cursor:
            if user is None:
                cursor.execute("RESET SESSION AUTHORIZATION")
            else:
                cursor.execute("SET SESSION AUTHORIZATION {}".format(user))
            cursor.execute("SET search_path TO public")
            cursor.execute(query)


@pytest.mark.encrypted_only
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.no_jdbc
@pytest.mark.custom_burst_gucs(gucs=CONSUMER_CLUSTER_GUCS)
@pytest.mark.usefixtures("customise_burst_cluster_datasharing")
@pytest.mark.customise_burst_cluster_args(BURST_CLUSTER_GUCS)
class TestDSBurstWriteEncrypt(DatasharingBurstTest, TestBurstWriteMVBase):
    def setup_consumer_cluster(self, consumer_cluster):
        run_query(consumer_cluster, 'dev',
                  "drop user if exists test_ds_bw_user;")
        run_query(consumer_cluster, 'dev',
                  "create user test_ds_bw_user with password 'Testing1234';")
        run_query(consumer_cluster, 'dev',
                  "Drop table if exists test_burst_tbl;")
        run_query(consumer_cluster, 'dev',
                  "CREATE table test_burst_tbl(id int) diststyle even;",
                  "test_ds_bw_user")
        run_query(consumer_cluster, 'dev',
                  "grant select on public.test_burst_tbl to public;")
        run_query(consumer_cluster, 'dev',
                  "GRANT USAGE ON SCHEMA ds_test_schema TO test_ds_bw_user;")

    def clean_consumer_cluster(self, consumer_cluster):
        run_query(consumer_cluster, 'dev',
                  "Drop table if exists test_burst_tbl cascade;")
        run_query(
            consumer_cluster, 'dev',
            "REVOKE USAGE ON SCHEMA ds_test_schema FROM test_ds_bw_user;")
        run_query(consumer_cluster, 'dev', "drop user test_ds_bw_user;")

    def run_query_on_burst(self,
                           consumer_cluster,
                           sql_query,
                           should_burst=True,
                           cs_status=None):
        consumer_conn_params = consumer_cluster.get_conn_params()
        with RedshiftDb(consumer_conn_params) as consumer_db, \
                consumer_db.cursor() as consumer_cursor:
            consumer_cursor.execute(
                "SET SESSION AUTHORIZATION test_ds_bw_user")
            consumer_cursor.execute("SET query_group to 'burst'")
            consumer_cursor.execute(sql_query)
            if should_burst:
                self._check_last_query_bursted(consumer_cluster,
                                               consumer_cursor)
            else:
                errcode = self._check_last_query_didnt_burst(
                    consumer_cluster, consumer_cursor)
                if cs_status:
                    assert cs_status == errcode, \
                            ("Expected cs_status = {}"
                             " but found {}").format(cs_status, errcode)

    def test_ds_burst_write(self, cluster, consumer_cluster,
                            datashare_context):
        """
            This test evaluates Datasharing and burst-write on consumer
            clusters. It disables burst-write for datasharing using a guc
            and checks if the guc works as expected.
        """
        self.setup_consumer_cluster(consumer_cluster)
        consumer_conn_params = consumer_cluster.get_conn_params()
        self._start_and_wait_for_refresh(consumer_cluster)
        with RedshiftDb(consumer_conn_params) as consumer_db, \
                consumer_db.cursor() as consumer_cursor:
            consumer_cursor.execute(
                "GRANT USAGE ON DATABASE {} TO public".format(
                    datashare_context.consumer_db_name))
            self.run_query_on_burst(consumer_cluster, DS_SQL)
            self.run_query_on_burst(
                consumer_cluster,
                INSERT_DS_SQL,
                should_burst=False,
                cs_status=66)
            self.run_query_on_burst(consumer_cluster, DS_SQL)
            self.clean_consumer_cluster(consumer_cluster)
