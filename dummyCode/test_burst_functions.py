# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import datetime
import uuid

from contextlib import contextmanager

from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_test import (BurstTest, get_burst_cluster_arn,
                                   setup_teardown_burst)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstFunctions(BurstTest):
    '''
    Class to test current functions, covered functions:
    current_user,
    current_user_id,
    current_database

    Functions not supported on Redshift tables:
    session_user
    current_schema
    current_schemas
    '''

    @contextmanager
    def snapshot_context(self, cluster, snapshot_identifier):
        cluster.backup_cluster(snapshot_identifier)
        yield
        RedshiftClusterHelper.delete_snapshot(snapshot_identifier)

    def validate_function(self, cluster, db_session, cursor, select_query,
                          function):
        '''
        Run a query on main cluster and burst and compare the results
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Build the dataset.
        table_name = "test_burst_functions_{}{}".format(
            function,
            str(uuid.uuid4().hex)[:4])
        cursor.execute(
            "CREATE TABLE {} (col1 varchar(255))".format(table_name))
        cursor.execute("INSERT INTO {} ({})".format(table_name, select_query))

        snapshot_identifier = ("testburstcurrentfunctions{}".format(
            str(uuid.uuid4().hex)[:4]))
        # Burst queries on main and burst and verify the results are the same.
        with self.snapshot_context(cluster, snapshot_identifier):
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))
            final_query = '''
            SELECT * FROM {} WHERE col1 = ("{}"()::text)
            '''.format(table_name, function)
            # Run a query on main to collect baseline
            cursor.execute("set query_group to noburst;")
            cursor.execute(final_query)
            main_result = cursor.fetch_scalar()
            qid = self.last_query_id(cursor)
            self.verify_query_didnt_bursted(cluster, qid)
            # Run a query on burst to collect actual results.
            cursor.execute("set query_group to burst;")
            cursor.execute(final_query)
            burst_result = cursor.fetch_scalar()
            qid = self.last_query_id(cursor)
            self.verify_query_bursted(cluster, qid)
            assert main_result == burst_result, (
                "Function {} is broken on burst".format(function))
            assert len(main_result) > 0, (
                "Function {} returns an empty result".format(function))

    @contextmanager
    def db_context(self, db_session, db_name, user_name):
        with db_session.cursor() as cursor:
            try:
                cursor.execute("create database {}".format(db_name))
                cursor.execute(
                    "create user {} password 'Testing1234'".format(user_name))
                yield
            finally:
                cursor.execute("drop database {}".format(db_name))
                cursor.execute("drop user {}".format(user_name))

    @pytest.mark.session_ctx(user_type='super')
    def test_burst_current_functions(self, cluster, db_session):
        """
        Test that functions on burst clusters return correct results
        when used in a predicate.
        Needs superuser to create database.
        """
        # Personalize burst cluster to allow refresh to happen
        burst_cluster_arn = cluster.list_acquired_burst_clusters()[0]
        cluster.personalize_burst_cluster(burst_cluster_arn)
        # Create a new DB such that the name is not as the default one
        dbname = "testburstcurrentfunctions{}".format(
            str(uuid.uuid4().hex)[:4])
        username = "burst_user_{}".format(str(uuid.uuid4().hex)[:4])
        with self.db_context(db_session, dbname, username):
            with RedshiftDb(cluster.get_conn_params(db_name=dbname)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SET SESSION AUTHORIZATION {}".format(username))
                    self.validate_function(cluster, db_session, cursor,
                                           "select current_user",
                                           "current_user")
                    self.validate_function(cluster, db_session, cursor,
                                           "select current_user_id",
                                           "current_user_id")
                    # For whatever reason current_database needs parenthesys
                    # after the function name. String concatenation is needed
                    # to avoid the "function not supported on Redshift tables"
                    # message.
                    self.validate_function(cluster, db_session, cursor,
                                           "SELECT '' || current_database()",
                                           "current_database")
