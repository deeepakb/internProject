# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import pytest
import getpass
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params

from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

CREATE_TABLE = """
CREATE TABLE test_table (
    l_orderkey int8 NOT NULL encode mostly32,
    l_partkey int4 NOT NULL,
    l_suppkey int4 NOT NULL,
    l_linenumber int4 NOT NULL encode delta,
    l_quantity numeric(12,2) NOT NULL encode bytedict,
    l_extendedprice numeric(12,2) NOT NULL encode mostly32,
    l_discount numeric(12,2) NOT NULL encode delta,
    l_tax numeric(12,2) NOT NULL encode delta,
    l_returnflag char(1) NOT NULL,
    l_linestatus char(1) NOT NULL encode runlength,
    l_shipdate timestamp NOT NULL encode delta,
    l_commitdate timestamp NOT NULL encode delta,
    l_receiptdate timestamp NOT NULL encode delta,
    l_shipinstruct char(25) NOT NULL encode bytedict,
    l_shipmode char(10) NOT NULL encode bytedict,
    l_comment varchar(44) NOT NULL encode text255,
    PRIMARY Key(L_ORDERKEY, L_LINENUMBER)
) diststyle KEY DISTKEY (L_ORDERKEY) sortkey(l_shipdate,l_orderkey) ;
"""

INSERT_CMD = """
COPY test_table FROM 's3://cookie-monster-s3-ingestion/\
table_partitions_at_ingest/lineitem_500000_rows/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3' GZIP;
"""

MODIFY_TABLE_PART_TEMPLATE = """
modify_table_partition test_table 2 {} {}
"""

GET_TABLE_ID = """
Select distinct id from stv_tbl_perm where name = 'test_table' limit 1;
"""
GET_TABLE_PARTITIONS_INFO = """
select slice, partition_id, rows, flags from stv_table_partitions \
where id = {} and rows > 0 order by 1, 2;
"""
GET_NUM_PARTITIONS = """
select count(distinct partition_id) from stv_table_partitions \
where id = (select 'test_table'::regclass::oid);
"""


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_tpcds("call_center")
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_local_gucs(
    gucs={
        'vacuum_part_size_multiplier': '0.001',
        'selective_dispatch_level': '0',
        'enable_remote_access_table_partitions_refresh': 'true'
    })
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstTablePartitionMetadataTransfer(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(unsorted_region=[
                'all_sorted',  # Many sorted partitions
                'all_unsorted',  # Many unsorted partitions
                'mixed_unsorted'  # Mix of sorted and unsorted partitions
            ]))

    def _setup_tables(self, cluster, vector):
        with self.db.cursor() as cursor:
            cursor.execute("drop table if exists test_table")
            cursor.execute(CREATE_TABLE)
            for i in range(5):
                cursor.execute(INSERT_CMD)
            if vector.unsorted_region == 'all_sorted':
                pass
            elif vector.unsorted_region == 'all_unsorted':
                for i in range(4):
                    cluster.run_xpx(
                        MODIFY_TABLE_PART_TEMPLATE.format(i + 1, 6))
                pass
            elif vector.unsorted_region == 'mixed_unsorted':
                for i in range(2):
                    cluster.run_xpx(
                        MODIFY_TABLE_PART_TEMPLATE.format(i + 1, 6))
            cursor.execute(GET_NUM_PARTITIONS)
            num_part = cursor.fetch_scalar()
            assert num_part > 2

    def _get_table_id(self):
        with self.db.cursor() as cursor:
            cursor.execute(GET_TABLE_ID)
            tbl_id = cursor.fetch_scalar()
            return tbl_id

    def _validate_table_partition_metadata(self, main_cursor, burst_cursor,
                                           tbl_id):
        res_from_main = main_cursor.execute(
            GET_TABLE_PARTITIONS_INFO.format(tbl_id))
        res_from_burst = burst_cursor.execute(
            GET_TABLE_PARTITIONS_INFO.format(tbl_id))
        assert res_from_main == res_from_burst

    def test_burst_table_partition_metadata_transfer(self, cluster, db_session,
                                                     vector):
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        self._setup_tables(cluster, vector)
        tbl_id = self._get_table_id()
        with self.db.cursor() as cur, \
                burst_session.cursor() as burst_cursor, \
                db_session.cursor() as q_cur:
            # Verify the table partitions are transfered for burst cold start
            self._validate_table_partition_metadata(cur, burst_cursor, tbl_id)
            # Run a query on burst to trigger burst refresh
            q_cur.execute("set query_group to burst")
            q_cur.execute("select count(*) from call_center")
            self.check_last_query_bursted(cluster, q_cur)
            self._validate_table_partition_metadata(cur, burst_cursor, tbl_id)
