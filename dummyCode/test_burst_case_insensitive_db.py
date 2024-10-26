# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.common.db.db_exception import OperationalError
from raff.common.db.session import (DbSession, _try_execute)
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest

__all__ = [super_simulated_mode]


# Query to get busrt error.
BURST_ERROR = '''
WITH errors (category, message, eventtime) AS (
    SELECT
        'PREPARE',
        btrim(error),
        starttime
    FROM
        stl_burst_prepare
    WHERE
        len(btrim(error)) > 0
        AND error NOT LIKE '%Query on backup%cannot run on burst since the last backup available is%'
        AND error NOT LIKE '%Query on backup%can burst once the current backup in progress%finishes%'
        AND error NOT LIKE '%BurstStateManagerError Cannot burst query on sb_version%'
        AND error NOT LIKE '%BurstStateManagerError Table % on sb version % has been modified by vacuum%'
    UNION
    SELECT
        action,
        btrim(error),
        eventtime
    FROM
        stl_burst_service_client
    WHERE
        error IS NOT NULL
    UNION
    SELECT
        action,
        reason,
        eventtime
    FROM
        stl_burst_service_client
    WHERE
        reason NOT IN (
            'Xpx',
            'Startup',
            'Shutdown',
            'IdleTimeout'
        )
        AND action LIKE '%RELEASE%'
    UNION
    SELECT
        'BACKUP',
        'Query on backup cannot run on burst since the last backup available is not sufficient',
        starttime
    FROM
        stl_burst_prepare
    WHERE
        error LIKE '%Query on backup%cannot run on burst since the last backup available is%'
    UNION
    SELECT
        'BACKUP',
        'Query on backup can burst once the current backup in progress finishes',
        starttime
    FROM
        stl_burst_prepare
    WHERE
        error LIKE '%Query on backup%can burst once the current backup in progress%finisstart_nodehes%'
    UNION
    SELECT
        'QUALIFICATION',
        'Query cant burst because of DML on the table',
        starttime
    FROM
        stl_burst_prepare
    WHERE
        error LIKE '%BurstStateManagerError Cannot burst query on sb_version % since DML%'
    UNION
    SELECT
        'QUALIFICATION',
        'Query cant burst because of vaccum on the table',
        starttime
    FROM
        stl_burst_prepare
    WHERE
        error LIKE '%BurstStateManagerError Table % on sb version % has been modified by vacuum%'
    UNION
    SELECT
        'EXECUTION',
        CASE
            WHEN error LIKE '%Numeric data overflow%' THEN 'Numeric data overflow'
            WHEN error LIKE '%JSON parsing error%' THEN 'JSON parsing error'
            WHEN error LIKE '%system requested abort%' THEN 'system requested abort'
            WHEN error LIKE '%Invalid digit%' THEN 'Invalid digit'
            WHEN error LIKE '%Invalid exponent%' THEN 'Invalid exponent'
            ELSE btrim(error)
        END AS error,
        starttime
    FROM
        stl_burst_query_execution
    WHERE
        len(btrim(error)) > 0
        AND error NOT LIKE '%Query%cancelled on user%s request%'
        AND error NOT LIKE '%Wlm queuing disabled%'
    UNION
    SELECT
        'CONNECTION',
        btrim(action) || '_' || btrim(error),
        eventtime
    FROM
        stl_burst_connection
    WHERE
        len(btrim(error)) > 0
    UNION
    SELECT
        'PERSONALIZATION',
        btrim(error),
        starttime
    FROM
        stl_burst_manager_personalization
    WHERE
        len(btrim(error)) > 0
    UNION
    SELECT
        'BILLING',
        btrim(message),
        recordtime
    FROM
        stl_concurrency_scaling_usage_error
    WHERE
        message NOT LIKE '%Cluster release reported on non-existent entry%'
    UNION
    SELECT
        'BILLING',
        'Cluster release reported on non-existent entry',
        recordtime
    FROM
        stl_concurrency_scaling_usage_error
    WHERE
        message LIKE '%Cluster release reported on non-existent entry%'
    UNION
    SELECT
        'WLM',
        btrim(error_string),
        recordtime
    FROM
        stl_wlm_error
    UNION
    SELECT
        'ERROR',
        CASE
            WHEN CONCAT (
                btrim(context),
                btrim(error)
            ) LIKE '%Numeric data overflow%' THEN 'Numeric data overflow'
            WHEN CONCAT (
                btrim(context),
                btrim(error)
            ) LIKE '%JSON parsing error%' THEN 'JSON parsing error'
            WHEN CONCAT (
                btrim(context),
                btrim(error)
            ) LIKE '%Invalid digit%' THEN 'Invalid digit'
            WHEN CONCAT (
                btrim(context),
                btrim(error)
            ) LIKE '%Invalid exponent%' THEN 'Invalid exponent'
            ELSE CONCAT (
                btrim(context),
                btrim(error)
            )
        END AS error,
        recordtime
    FROM
        stl_error
    WHERE
        error ilike '%burst%'
        OR context ilike '%burst%'
        OR FILE ilike '%burst%'
        AND error NOT LIKE '%Wlm queuing disabled%'
    UNION
    SELECT
        'TRACE',
        CASE
            WHEN message LIKE '%Numeric data overflow%' THEN 'Numeric data overflow'
            WHEN message LIKE '%JSON parsing error%' THEN 'JSON parsing error'
            WHEN message LIKE '%system requested abort%' THEN 'system requested abort'
            WHEN message LIKE '%Invalid digit%' THEN 'Invalid digit'
            WHEN message LIKE '%Invalid exponent%' THEN 'Invalid exponent'
            ELSE btrim(message)
        END AS message,
        eventtime
    FROM
        stl_event_trace
    WHERE
        event_name ilike '%burst%'
        AND message NOT LIKE '%Query%cancelled on user%s request%'
        AND message NOT LIKE '%TakeBackup failed for backupId%'
        AND message NOT LIKE '%Wlm queuing disabled%'
        AND LEVEL >= 17 -- level 17 is WARNING
    UNION
    SELECT
        'BACKUP',
        'TakeBackup failed for backupId with CURL ' || regexp_substr(message, 'code .*'),
        eventtime
    FROM
        stl_event_trace
    WHERE
        message LIKE '%TakeBackup failed for backupId%'
    UNION
    SELECT
        'REFRESH',
        btrim(error),
        eventtime
    FROM
        stl_burst_manager_refresh
    WHERE
        len(btrim(error)) > 0
)
SELECT
    category,
    max(eventtime),
    count(*),
    message
FROM
    errors
WHERE
    message NOT LIKE '%Shutdown%End of file%'
    AND message NOT LIKE '%Shutdown%system lib%'
GROUP BY 1, 4
ORDER BY 2 DESC;
'''


def try_drop_database(cursor, dbname):
    drop_db_sql = "DROP DATABASE {}".format(dbname)
    _try_execute(drop_db_sql, cursor, 'dropping db', [OperationalError])


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCaseInsensitiveDB(BurstWriteTest):
    def test_burst_spectrum_in_ci_db(self, cluster, db_session):
        '''This test is to make sure guc case_sensitive is serialized
           correctly to burst cluster. For case insensitive database,
           which guc case_sensitive is set to false, string comparison
           should be case insenstiive for Spectrum query.'''
        # Create a case insensitive database.
        with self.db.cursor() as db_cursor:
            db_cursor.execute('Create database ci_database collate '
                              'case_insensitive')
            db_cursor.execute('GRANT ALL on database ci_database to PUBLIC')

        ci_session = DbSession(cluster.get_conn_params(db_name='ci_database'))
        with ci_session.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS s3dory")
            cursor.execute("CREATE EXTERNAL SCHEMA s3dory FROM DATA CATALOG "
                           "DATABASE 'default' REGION 'us-west-2' IAM_ROLE"
                           " 'arn:aws:iam::467896856988:role/Redshift-S3'")
            cursor.execute("GRANT USAGE ON SCHEMA s3dory TO public")
            self._start_and_wait_for_refresh(cluster)
            # Run a Spectrum query. This query should burst and string
            # comparison should be case-insensitive.
            cursor.execute("set query_group to burst")
            cursor.execute("select cvarchar from s3dory.alltypes_csv "
                           "where cvarchar = 'MENG'")
            assert cursor.fetchall() == [('Meng',), ('Meng',)]
            cursor.execute("select pg_last_query_id()")
            last_query_id = cursor.fetch_scalar()
            # Make sure query should burst.
            cursor.execute("select concurrency_scaling_status from stl_query "
                           "where query = {}".format(last_query_id))
            status = cursor.fetchall()
            if status != [(1,)]:
                ci_session.close()
                with self.db.cursor() as db_cursor:
                    db_cursor.execute(
                        "select concurrency_scaling_status_txt from "
                        "svl_query_concurrency_scaling_status "
                        "where query = {}".format(last_query_id))
                    status_txt = db_cursor.fetch_scalar()
                    # Also get burst error if there is any.
                    db_cursor.execute(BURST_ERROR)
                    detail = db_cursor.fetchall()
                    raise Exception(
                        "Test failed. concurrency_scaling_status: {} \n"
                        "Detail: {}".format(status_txt, detail))
        ci_session.close()

        with self.db.cursor() as db_cursor:
            # Drop database.
            try_drop_database(db_cursor, 'ci_database')
