# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.burst.burst_test import setup_teardown_burst
from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]


class BWErrorHandlePGErrorSig11(TestBurstWriteMixedWorkloadBase):
    def error_handle_pgerror_sig_11(self, cluster, vector):
        """
        The test is to repro issue:
        """
        test_schema = 'test_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        base_tbl_name = "dp28532"

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            self._setup_tables(cluster, session1, base_tbl_name, vector)
            self._start_and_wait_for_refresh(cluster)
            dml_cursor = session1.cursor()
            read_sql = "select * from {} limit 10000".format(base_tbl_name)
            declare_raw = "declare burst_cursor cursor with hold for {}"
            declare_sql = declare_raw.format(read_sql)
            fetch_sql = "fetch forward 100 from burst_cursor;"
            dml_cursor.execute("set query_group to burst;")
            dml_cursor.execute("BEGIN;")
            dml_cursor.execute(declare_sql)
            for i in range(90):
                dml_cursor.execute(fetch_sql)
            fetch_fail = False
            try:
                with cluster.event("EtBurstThrowPGException"):
                    for i in range(10):
                        dml_cursor.execute(fetch_sql)
            except Exception as e:
                if "Prematurely failed with PG exception" in str(e):
                    fetch_fail = True
            assert fetch_fail
            dml_cursor.execute("COMMIT;")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'vacuum_auto_worker_enable': 'false',
        "enable_result_cache_for_session": 'false'
    })
@pytest.mark.custom_local_gucs(gucs={
    "enable_result_cache_for_session": 'false'
})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteErrorHandlePGErrorSig11SS(BWErrorHandlePGErrorSig11):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even'], sortkey=[''], size=['large']))

    def test_burst_write_error_handle_pgerror_sig_11(self, cluster, vector):
        self.error_handle_pgerror_sig_11(cluster, vector)
