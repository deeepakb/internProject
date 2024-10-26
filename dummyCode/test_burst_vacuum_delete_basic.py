# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from base_burst_vacuum import (BaseBurstVacuumSuite,
                               ENABLE_BURST_VACUUM_DELETE)
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode, get_burst_conn_params)
from raff.common.cluster.cluster_defaults import LocalhostDefault

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
CHECK_PHYSICAL_ROW_NUM = "select deletexid, count(*) from {}.{} group by 1"
BURST_VACUUM_QUERY_STL = (
    "select count(*) from stl_query where starttime >= '{}'"
    " and querytxt ilike '%vacuum%'")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_VACUUM_DELETE)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_VACUUM_DELETE)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBursVacuumDeleteBasic(BaseBurstVacuumSuite):
    _SCHEMA = None

    def _get_physical_row_cnt(self, tbl_name):
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(
                CHECK_PHYSICAL_ROW_NUM.format(self._SCHEMA, tbl_name))
            row_cnt = bootstrap_cursor.fetch_scalar()
            return row_cnt

    def validate_bursted_vacuum(self, cursor, starttime):
        cursor.execute(BURST_VACUUM_QUERY_STL.format(starttime))
        assert cursor.fetch_scalar() > 0

    def test_burst_user_vacuum_delete_basic(self, cluster):
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        db_session_burst = RedshiftDb(
            get_burst_conn_params(user=LocalhostDefault.BOOTSTRAP_USER))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_vacuum_table(db_session_master)
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            cursor.execute("DELETE FROM t1 WHERE c4 % 2 = 0;")
            cursor.execute("select now()")
            starttime = cursor.fetch_scalar()
            gold_res = self.validate_table_correctness(self._SCHEMA, "t1")
            self._start_and_wait_for_refresh(cluster)
            cluster.set_event("EtForceBurstVacuum")
            self.setup_try_burst_first_session(cursor)
            cursor.execute("vacuum delete only t1 to 100 percent")
            cur_res = self.validate_table_correctness(self._SCHEMA, "t1")
            assert gold_res == cur_res, "{} diff {}".format(gold_res, cur_res)
        with db_session_burst.cursor() as burst_cursor:
            self.validate_bursted_vacuum(burst_cursor, starttime)
