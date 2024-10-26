import pytest
import uuid

from contextlib import contextmanager
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)
from raff.common.db.redshift_db import RedshiftDb

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "verify_query_bursted"]

CREATE_QUERY = '''
CREATE TABLE dp25176 (
  ci_varchar varchar(10) COLLATE case_insensitive,
  cs_varchar varchar(10) COLLATE case_sensitive,
  ci_char char(20) COLLATE case_insensitive,
  cs_char char(20) COLLATE case_sensitive,
  cint int,
  cbigint bigint);

CREATE TABLE dp25176_1 (
  ci_varchar varchar(10) COLLATE case_insensitive
);

CREATE TABLE dp25541_2 (
  ci_varchar  varchar(10) COLLATE ci,
  ci_char  char(10) COLLATE ci,
  cint int
);

CREATE TABLE dp25541_3 (
  ci_varchar  varchar(10) COLLATE ci,
  ci_char  char(10) COLLATE ci distkey,
  cint int
);
CREATE TABLE dp25541_4 (
  ci_varchar  varchar(10) COLLATE ci,
  ci_char  char(10) COLLATE ci distkey,
  cint int
);

CREATE TABLE dp25541_5 (
  ci_varchar  varchar(10) COLLATE ci distkey sortkey
);
CREATE TABLE dp25541_6 (
  ci_varchar  varchar(10) COLLATE ci distkey sortkey
);
'''

INSERT_QUERY = '''
INSERT INTO dp25176 VALUES
('amazon', 'AMAZON', 'hello world', 'hELLO world', 0, 0),
('Amazon', 'amaZON', 'hello WOrld', 'hellO World', 1, 1),
('AMAZON', 'AMAzon', 'HELLo world', 'HELLO world', 2, 2),
('AmaZon', 'amaZon', 'heLLo WOrlD', 'Hello World', 3, 3);

INSERT INTO dp25176_1 VALUES ('a');

INSERT INTO dp25541_2 VALUES
('b', 'A', 0),
('B', 'b', 1),
('a', 'C', 2),
('D', 'd', 3),
('b', 'E', 4);

INSERT INTO dp25541_3 VALUES
('b', 'B', 0);
INSERT INTO dp25541_4 VALUES
('b', 'B', 1),
('B', 'b', 2);

INSERT INTO dp25541_5 VALUES ('b'), ('a');
INSERT INTO dp25541_6 VALUES ('B'), ('A');
'''

CLEANUP_QUERY = '''
DROP TABLE IF EXISTS dp25176;
DROP TABLE IF EXISTS dp25176_1;
DROP TABLE IF EXISTS dp25541_2;
DROP TABLE IF EXISTS dp25541_3;
DROP TABLE IF EXISTS dp25541_4;
DROP TABLE IF EXISTS dp25541_5;
DROP TABLE IF EXISTS dp25541_6;
'''


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.session_ctx(user_type='super')
@pytest.mark.custom_burst_gucs(gucs={'case_sensitive': 'false',
                                     'enable_column_level_collation': 'true'})
class TestBurstColumnCollation(BurstTest):
    def generate_sql_res_files(self):
        return True

    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def run_test(self, db_session, cluster, testfile):
        """
        Backup tables and run burst test queries.
        """
        # Set up testing tables and backup.
        with db_session.cursor() as cursor:
            cursor.execute(CLEANUP_QUERY)
            cursor.execute(CREATE_QUERY)
            cursor.execute(INSERT_QUERY)

        with self.auto_release_local_burst(cluster):
            SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                    str(uuid.uuid4().hex)))
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
            # Run test workload.
            with self.burst_db_session(db_session) as cursor:
                self.execute_test_file(testfile, session=db_session)
        # Clean up.
        with db_session.cursor() as cursor:
            cursor.execute(CLEANUP_QUERY)

    def test_burst_column_collation(self, db_session, cluster,
                                    verify_query_bursted):
        self.run_test(db_session, cluster, 'burst_column_collation')

    @pytest.mark.create_external_schema
    def test_column_collation_spectrum_volt(self, db_session, cluster):
        """
        Test bursting Spectrum query and query with volt tt.
        """
        self.run_test(db_session, cluster, 'column_collation_spectrum_volt')
