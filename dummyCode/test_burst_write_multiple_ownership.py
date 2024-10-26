# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
import uuid
import datetime

from contextlib import contextmanager

from raff.burst.burst_write import BurstWriteTest
from raff.common.dimensions import Dimensions
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_basic_gucs
from raff.common.base_test import run_priviledged_query
from raff.common.base_test import run_priviledged_query_scalar_int
from raff.burst.burst_unload.burst_unload_test_helper import AllDatatypesQuery
from raff.burst.burst_unload.burst_unload_test_helper import TEST_S3_PATH
from raff.burst.burst_write import IAM_WRITE_CREDENTIALS
from raff.burst.burst_test import get_burst_cluster

log = logging.getLogger(__name__)
__all__ = [setup_teardown_burst]


@pytest.mark.serial_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_write_basic_gucs.items()) + [(
            'enable_burst_async_acquire', 'false'), (
                'burst_commit_refresh_check_frequency_seconds',
                '-1'), ('enable_burst_lag_based_background_refresh',
                        'false'), ('always_burst_eligible_query', 'false')]))
@pytest.mark.cluster_only
@pytest.mark.load_data
class TestBurstMultipleOwnership(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(cmd=['insert', 'delete', 'update', 'copy']))

    @contextmanager
    def create_burstable_table(self, cluster, db_session, tbl_name):
        create = '''
                 create table if not exists {}(col1 int) diststyle even
                 '''.format(tbl_name)
        insert = '''
                 insert into {} values (1)
                 '''.format(tbl_name)
        drop = '''
               drop table {}
               '''.format(tbl_name)
        with db_session.cursor() as cur:
            cur.execute("SET query_group TO burst")
            cur.execute(create)
            cur.execute(insert)
            self._start_and_wait_for_refresh(cluster)
            yield
            cur.execute(drop)

    def create_copy_cmd(self, db_session, tbl_name):
        unload_path = TEST_S3_PATH.format("credentials_test",
                                          self._generate_random_string())

        with db_session.cursor() as cur:
            # Populate some data in s3
            cur.run_unload(
                select_stmt=AllDatatypesQuery.SELECT_STMT,
                data_dest=unload_path,
                auth=IAM_WRITE_CREDENTIALS)
            cur.execute("create table {} (like {})".format(
                tbl_name, AllDatatypesQuery.TBL))
            dml = cur.construct_copy_command(tbl_name, unload_path,
                                             IAM_WRITE_CREDENTIALS)
            return dml

    def take_backup(self, cluster):
        snapshot_id = "burst-write-snapshot-{}".format(str(uuid.uuid4().hex))
        cluster.backup_cluster(snapshot_id)

    def verify_table_ownership(self, cluster, schema, tbl, state, arn):
        sql = '''
              select trim(state)
              from stv_burst_tbl_ownership
              where id='{}.{}'::regclass::oid
              and arn like '%{}%'
              '''.format(schema, tbl, arn)
        with self.db.cursor() as cur:
            res = run_priviledged_query(cluster, cur, sql)
            log.info("Ownership result for arn {}= {}".format(arn, res))

            success = False
            if state is None and len(res) == 0:
                success = True
            elif state is not None and len(res) > 0 and res[0][0] == state:
                success = True
            if not success:
                sql = '''
                select * from stv_burst_tbl_ownership
                '''
                full_res = run_priviledged_query(cluster, cur, sql)
                log.info("Ownership result = {}".format(full_res))
                actual_state = "none" if len(res) == 0 else res[0][0]
                assert False, ("Expected {} for table {}, but found {}".format(
                    state, tbl, actual_state))

    def verify_query_cluster(self, cluster, query, arn):
        sql = '''
              select count(*)
              from stl_burst_query_execution
              where query = {}
              and cluster_arn like '%{}%'
              '''.format(query, arn)
        with self.db.cursor() as cur:
            count = run_priviledged_query_scalar_int(cluster, cur, sql)
            log.info(
                "stl_burst_query_execution entries for query {}= {}".format(
                    query, count))
            if count == 0:
                sql = '''
                select * from stl_burst_query_execution where query = {}
                '''.format(query)
                full_res = run_priviledged_query(cluster, cur, sql)
                log.info("stl_burst_query_execution entries for query {} = {}".
                         format(query, full_res))
                assert False, (
                    "Expected {} to run on {}, but query ran on a different cluster".
                    format(query, arn))

    def get_client_logs(self, cluster, starttime):
        sql = """
        select pid,
               xid,
               btrim(action),
               btrim(cluster_arn),
               eventtime,
               num_nodes,
               btrim(reason),
               btrim(error)
               from stl_burst_service_client
               where eventtime >= '{}'
               order by eventtime desc
               limit 6;
        """.format(starttime)
        with self.db.cursor() as cur:
            res = run_priviledged_query(cluster, cur, sql)
            log.info("Service client result = {}".format(res))
            return res

    def get_execution_cluster(self, cluster, qid):
        sql = """
        select btrim(cluster_arn)
               from stl_burst_query_execution
               where query = {}
        """.format(qid)
        with self.db.cursor() as cur:
            res = run_priviledged_query(cluster, cur, sql)
            log.info("stl_burst_query_execution result = {}".format(res))
            return res[0][0]

    def test_burst_multiple_ownership_busy(self, cluster, db_session, vector):
        '''
        Disable Commit Based Cold Start in this test.
        When cluster is busy, query can trigger cold start and run on
        new burst cluster with stale backup when CBC is enabled. Thus we
        cannot test that query can only run on main when burst cluster
        is busy.
        1) Write on BC1, table1 -> BC1 owns table1
        2) Set event to simulate BC1 busy
        3) Read on table1 -> Can't burst except on BC1, but the cluster is busy
        4) Write on table1 -> Can't burst except on BC1, but the cluster is busy
        5) Backup
        6) Read on table1 -> Can burst on BC2
        7) Write on table1 -> Can burst on BC2
        8) Set event to simulate BC2 busy, unset for BC1
        9) Read on table1 -> Can't burst except on BC2, but the cluster is busy
        10) Write on table1 -> Can't burst except on BC2, but the cluster is busy
        11) Backup and refresh BC1
        12) Unset event for BC2
        13) Read on table1 -> Can burst on BC1,BC2
        14) Write on table1 -> Can burst on BC1,BC2

        15) Run write query on BC1 and abort.
        16) Read on table1 -> Cannot burst on BC1,BC2 because of the abort
        17) Write on table1 -> Cannot burst on BC1,BC2 because of the abort
        18) Backup and refresh BC1
        19) Write on table1 -> Can burst on BC1,BC2
        20) Read on table1 -> Can burst on BC1,BC2

        21) Vacuum table1.
        22) Read on table1 -> Cannot burst on BC1,BC2 because of the vacuum
        23) Write on table1 -> Cannot burst on BC1,BC2 because of the vacuum
        24) Backup and refresh BC1
        25) Write on table1 -> Can burst on BC1,BC2
        26) Read on table1 -> Can burst on BC1,BC2
        '''

        schema = db_session.session_ctx.schema
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        tbl_name = "test_burst_multiple_ownership_busy"
        if vector.cmd == 'insert':
            dml = '''
                  insert into {} select * from {}
                  '''.format(tbl_name, tbl_name)
        if vector.cmd == 'update':
            dml = '''
                  update {} set col1 = 2
                  '''.format(tbl_name)
        if vector.cmd == 'delete':
            dml = '''
                  delete from {} where col1 = 2
                  '''.format(tbl_name)
        if vector.cmd == 'copy':
            dml = self.create_copy_cmd(db_session, tbl_name)
        select = '''
                 select * from {}
                 '''.format(tbl_name)
        with self.create_burstable_table(cluster, db_session, tbl_name), \
                db_session.cursor() as cur:
            cur.execute("set query_group to burst")
            arn1 = self.get_latest_acquired_cluster(cluster)
            burst_cluster_arn = cluster.list_acquired_burst_clusters()
            assert len(burst_cluster_arn) > 0, "No burst clusters acquired"
            log.info("Number of burst clusters = {}".format(
                len(burst_cluster_arn)))
            log.info("Burst cluster = {}".format(burst_cluster_arn[0]))
            # 1) Write on BC1, table1 -> BC1 owns table1
            self._start_and_wait_for_refresh(cluster)
            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            self.verify_query_cluster(cluster, qid, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn1)

            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            self.verify_query_cluster(cluster, qid, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn1)

            # 2) Set event to simulate BC1 busy
            with cluster.event("EtFakeBurstClusterSessions",
                               "arn={}".format(arn1)):
                # 3 Read on table1 -> Can't burst except on BC1, but the cluster is busy
                cur.execute(select)
                qid = self.last_query_id(cur)
                self.verify_query_status(cluster, qid, 0)
                self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                            arn1)

                # 4) Write on table1 -> Can't burst except on BC1, but the cluster is
                # busy
                cur.execute(dml)
                qid = self.last_query_id(cur)
                self.verify_query_status(cluster, qid, 0)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)

                # Refresh BC2 so it can burst on BC2
                self._start_and_wait_for_refresh(cluster)
                # 6) Read on table1 -> Can burst on BC2
                cur.execute(select)
                qid = self.last_query_id(cur)
                self.verify_query_bursted(cluster, qid)
                arn2 = self.get_latest_acquired_cluster(cluster)
                assert arn1 != arn2, "No new cluster acquired: {}".format(
                    self.get_client_logs(cluster, start_str))
                self.verify_query_cluster(cluster, qid, arn2)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn2)

                # 7) Write on table1 -> Can burst on BC2
                cur.execute(dml)
                qid = self.last_query_id(cur)
                self.verify_query_bursted(cluster, qid)
                self.verify_query_cluster(cluster, qid, arn2)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)
                self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                            arn2)

            # 8) Set event to simulate BC2 busy, unset for BC1
            with cluster.event("EtFakeBurstClusterSessions",
                               "arn={}".format(arn2)):
                # 9) Read on table1 -> Can't burst except on BC2, but the cluster is
                # busy
                cur.execute(select)
                qid = self.last_query_id(cur)
                self.verify_query_status(cluster, qid, 0)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)
                self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                            arn2)

                # 10) Write on table1 -> Can't burst except on BC2, but the cluster is
                # busy
                cur.execute(dml)
                qid = self.last_query_id(cur)
                self.verify_query_status(cluster, qid, 0)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn2)

            # 11) Backup and refresh BC1
            self._start_and_wait_for_refresh(cluster)

            # 13) Read on table1 -> Can burst on BC1,BC2
            cur.execute(select)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn2)

            # 14) Write on table1 -> Can burst on BC1,BC2
            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            owning_arn = arn
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)

            # Set the limit of burst we can acquire to 2, otherwise
            # the query will run on a 3rd cluster.
            cluster.run_xpx('burst_update_max_clusters 2')

            # 15) Run write query on BC1 and abort.
            # Abort is simulated using EtSimulateRestAgentFetchError
            burst_cluster = get_burst_cluster(arn)
            with burst_cluster.event('EtSimulateRestAgentFetchError'):
                regex = 'Could not execute query'
                qstarttime = datetime.datetime.now().replace(microsecond=0)
                qstart_str = qstarttime.isoformat(' ')
                cur.execute_failing_query(dml, regex)
                get_query = '''
                            select query from stl_query
                            where userid > 1
                            and starttime >= '{}'
                            order by starttime asc
                            limit 1
                            '''.format(qstart_str)
                qid = run_priviledged_query_scalar_int(cluster, cur, get_query)
                # 25 = Ineligible to rerun on main cluster due to
                # failure handling not enabled
                self.verify_query_status(cluster, qid, 25)
                arn = self.get_execution_cluster(cluster, qid)
                assert arn == owning_arn
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn1)
                self.verify_table_ownership(cluster, schema, tbl_name, None,
                                            arn2)

            # 16) Read on table1 -> Cannot burst on BC1,BC2 because of the abort
            cur.execute(select)
            qid = self.last_query_id(cur)
            self.verify_query_status(cluster, qid, 0)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn2)

            # 17) Write on table1 -> Cannot burst on BC1,BC2 because of the abort
            cur.execute(dml)
            qid = self.last_query_id(cur)
            # error 1
            self.verify_query_status(cluster, qid, 0)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn2)

            # 18) Backup and refresh BC1
            self._start_and_wait_for_refresh(cluster)

            # 19) Write on table1 -> Can burst on BC1,BC2
            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            owning_arn = arn
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)

            # 20) Read on table1 -> Can burst on BC1,BC2
            cur.execute(select)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            assert arn == owning_arn
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)

            # 21) Run vacuum on table1
            cur.execute("vacuum {} to 100 percent".format(tbl_name))
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))

            # 22) Read on table1 -> Cannot burst on BC1,BC2 because of the vacuum
            cur.execute(select)
            qid = self.last_query_id(cur)
            self.verify_query_status(cluster, qid, 1)
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)
            self.verify_table_ownership(cluster, schema, tbl_name,
                                        "Structure Changed", 'Main')
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))

            # 23) Write on table1 -> Cannot burst on BC1,BC2 because of the vacuum
            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_status(cluster, qid, 0)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn1)
            self.verify_table_ownership(cluster, schema, tbl_name, None, arn2)

            # 24) Backup and refresh BC1
            self._start_and_wait_for_refresh(cluster)

            # 25) Write on table1 -> Can burst on BC1,BC2
            cur.execute(dml)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            owning_arn = arn
            assert arn in [
                arn1, arn2
            ], ("Query {} executed on {} instead of {} or {}".format(
                qid, arn, arn1, arn2))
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)

            # 26) Read on table1 -> Can burst on BC1,BC2
            cur.execute(select)
            qid = self.last_query_id(cur)
            self.verify_query_bursted(cluster, qid)
            arn = self.get_execution_cluster(cluster, qid)
            assert arn == owning_arn
            self.verify_table_ownership(cluster, schema, tbl_name, "Owned",
                                        arn)

        cluster.run_xpx('burst_release_all')
