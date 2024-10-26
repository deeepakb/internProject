# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

CUSTOM_GUCS = {
    'enable_runtime_filtering': 'true',
    'runtime_filtering_explain': 'true',
    'enable_runtime_filtering_in_burst': 'true',
    'burst_use_local_scans': 'true'
}

SQL_DDL1 = """CREATE TABLE dist_rtf_t1(a int, b int) diststyle even;
              CREATE TABLE dist_rtf_t2(a int, b int) diststyle even;
              CREATE TABLE dist_rtf_t3(a int, b int) diststyle ALL;

              INSERT INTO dist_rtf_t1 VALUES(1, 1);
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t1
              SELECT a + max(a) over(), b + max(b) over() FROM dist_rtf_t1;

              INSERT INTO dist_rtf_t2 SELECT * FROM dist_rtf_t1;
              INSERT INTO dist_rtf_t3 SELECT * FROM dist_rtf_t1;"""

SQL_QID = "select pg_last_query_id();"


SQL_VALIDATE_PLAN = """select substring(label, 1, 7)
                       from svl_query_summary
                       where query IN (
                         SELECT concurrency_scaling_query
                         FROM stl_concurrency_scaling_query_mapping
                         WHERE primary_query = {}
                       ) order by stm, seg, step;"""

SQL_VALIDATE_RTF = """
SELECT
  s.segment,
  s.step,
  rtf.hash_segment,
  rtf.hash_step,
  sum(rtf.checked) as total_checked,
  sum(rtf.rejected) as total_rejected
FROM stl_rtf_stats rtf
  RIGHT JOIN stl_scan s
          ON rtf.segment = s.segment
         AND rtf.step = s.step
         AND rtf.query = s.query
WHERE s.query in (
  select concurrency_scaling_query
  from stl_concurrency_scaling_query_mapping
  where primary_query = {})
AND   s.runtime_filtering = 't'
group by 1,2,3,4
ORDER BY 1,2,3,4;
"""

ERROR_PLAN = "unexpected execution steps"
ERROR_RTF = "unexpected runtime filter stats"

SQL_Q1 = """SELECT *
         FROM dist_rtf_t1 t1
           JOIN dist_rtf_t2 t2 ON t1.a = t2.a
         WHERE t1.a % 2 = 0
         AND   t2.a % 3 = 0
         ORDER BY 1, 2, 3, 4;"""
RES_Q1 = [(6, 6, 6, 6),
          (12, 12, 12, 12),
          (18, 18, 18, 18),
          (24, 24, 24, 24),
          (30, 30, 30, 30),
          (36, 36, 36, 36),
          (42, 42, 42, 42),
          (48, 48, 48, 48),
          (54, 54, 54, 54),
          (60, 60, 60, 60),
          (66, 66, 66, 66),
          (72, 72, 72, 72),
          (78, 78, 78, 78),
          (84, 84, 84, 84),
          (90, 90, 90, 90),
          (96, 96, 96, 96),
          (102, 102, 102, 102),
          (108, 108, 108, 108),
          (114, 114, 114, 114),
          (120, 120, 120, 120),
          (126, 126, 126, 126)]

RES_Q1_PLAN = [('scan   ',), ('project',), ('dist   ',),
               ('scan   ',), ('project',), ('hash   ',),
               ('scan   ',), ('bcast  ',),
               ('scan   ',), ('save   ',),
               ('scan   ',), ('project',), ('dist   ',),
               ('scan   ',), ('project',), ('hjoin  ',), ('project',),
               ('sort   ',),
               ('scan   ',), ('return ',),
               ('merge  ',), ('project',), ('return ',)]

RES_Q1_RTF = [(4, 0, 1, 2, 384, 258)]

SQL_Q2 = """SELECT *
            FROM dist_rtf_t1 t1
              JOIN dist_rtf_t2 t2 ON t1.a = t2.a
              JOIN dist_rtf_t3 t3 ON t1.a = t3.a
              JOIN dist_rtf_t1 t4 ON t1.b = t4.b
              JOIN dist_rtf_t3 t5 ON t1.a = t5.a
            WHERE t1.a % 2 = 0
            AND   t2.a % 3 = 0
            AND   t3.a % 4 = 0
            AND   t4.b % 2 = 0
            AND   t5.a % 3 = 0
            ORDER BY t1.a, t2.a, t3.a, t4.a, t5.a;"""

RES_Q2 = [(12, 12, 12, 12, 12, 12, 12, 12, 12, 12),
          (24, 24, 24, 24, 24, 24, 24, 24, 24, 24),
          (36, 36, 36, 36, 36, 36, 36, 36, 36, 36),
          (48, 48, 48, 48, 48, 48, 48, 48, 48, 48),
          (60, 60, 60, 60, 60, 60, 60, 60, 60, 60),
          (72, 72, 72, 72, 72, 72, 72, 72, 72, 72),
          (84, 84, 84, 84, 84, 84, 84, 84, 84, 84),
          (96, 96, 96, 96, 96, 96, 96, 96, 96, 96),
          (108, 108, 108, 108, 108, 108, 108, 108, 108, 108),
          (120, 120, 120, 120, 120, 120, 120, 120, 120, 120)]

RES_Q2_PLAN = [('scan   ',), ('project',), ('project',), ('hash   ',),
               ('scan   ',), ('project',), ('project',), ('hash   ',),
               ('scan   ',), ('project',), ('dist   ',),
               ('scan   ',), ('project',), ('hash   ',),
               ('scan   ',), ('project',), ('dist   ',),
               ('scan   ',), ('project',), ('hash   ',),
               ('scan   ',), ('bcast  ',),
               ('scan   ',), ('save   ',),
               ('scan   ',), ('project',), ('dist   ',),
               ('scan   ',), ('project',), ('hjoin  ',), ('project',),
               ('dist   ',),
               ('scan   ',), ('project',), ('hjoin  ',), ('project',),
               ('project',),
               ('hjoin  ',), ('project',), ('project',), ('hjoin  ',),
               ('project',),
               ('sort   ',),
               ('scan   ',), ('return ',),
               ('merge  ',), ('project',), ('return ',)]

RES_Q2_RTF = [(1, 0, 0, 3, 756, 576),
              (2, 0, 1, 3, 252, 192),
              (8, 0, 3, 2, 384, 324),
              (8, 0, 5, 2, 384, 0)]


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
class TestBurstRtfLocalScan(BurstTest):
    def test_burst_rtf_local_scan(self, cursor, db_session, cluster):
        # Prepare data.
        with db_session.cursor() as cursor, \
             self.auto_release_local_burst(cluster):
            cursor.execute(SQL_DDL1)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)

        qid = None

        # Execute Q1 and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(SQL_Q1)
            assert cursor.fetchall() == RES_Q1
            cursor.execute(SQL_QID)
            qid = cursor.fetchall()[0][0]

        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_PLAN.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q1_PLAN, \
                ERROR_PLAN
            bootstrap_cursor.execute(SQL_VALIDATE_RTF.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q1_RTF, \
                ERROR_RTF

        # Execute Q2 and verify.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(SQL_Q2)
            assert cursor.fetchall() == RES_Q2
            cursor.execute(SQL_QID)
            qid = cursor.fetchall()[0][0]

        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SQL_VALIDATE_PLAN.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q2_PLAN, \
                ERROR_PLAN
            bootstrap_cursor.execute(SQL_VALIDATE_RTF.format(qid))
            assert bootstrap_cursor.fetchall() == RES_Q2_RTF, \
                ERROR_RTF
