# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import os
from contextlib import contextmanager

from raff.common.cluster.cluster_session import ClusterSession
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.node_type import NodeType

from raff.redcat.redcat_test import RedCatTest
from raff.ddm.ddm_test import DDMTest
from raff.datasharing.datasharing_test import (DatasharingTest,
                                               enable_basic_datasharing_gucs,
                                               datasharing_write_enabled_gucs)
from raff.super_simulated.super_simulator import (ssm_datashare,
                                                  ssm_consumer_cluster)

__all__ = ['enable_basic_datasharing_gucs']

pytestmark = pytest.mark.node_info(
    launch_node_type=NodeType.I3EN_XLPLUS,
    unsupported_node_types=NodeType.non_ra3_types)

FILES_DIRECTORY = "datasharing"
SHARE_NAME = 'simulated_share'
IMPORTED_DB_NAME = 'consumer_db'
FEATURE_OFF = {'ddm_enable_for_datashares': 'false'}
FEATURE_ON = {'ddm_enable_for_datashares': 'true'}
BU_CONTEXT = SessionContext(user_type='bootstrap')
REL_MAP = {
    'table': 'TABLE',
    'view': 'VIEW',
    'lbv': 'VIEW',
    'mv': 'MATERIALIZED VIEW'
}


def fullpath(file):
    return os.path.join(FILES_DIRECTORY, file)


def get_alter_relkind(relkind):
    '''
    Get the relation kind for the ALTER command.
    - table, view, and LBV -> ALTER TABLE
    - materialized view    -> ALTER MATERIALIZED VIEW
    '''
    return "mv" if relkind == "mv" else "table"


def get_lbv_suffix(relkind):
    '''
    Get the CREATE VIEW late binding view suffix.
    '''
    return "WITH NO SCHEMA BINDING" if relkind == "lbv" else ""


@pytest.mark.serial_only  # uses fixed dbnames, sets up GUCs.
@pytest.mark.isolate_db
@pytest.mark.usefixtures('enable_ddm', 'enable_ddm_datasharing')
@pytest.mark.datasharing
class TestDDMDatasharing(DDMTest, DatasharingTest, RedCatTest):
    '''
    Test suite to verify DDM datasharing semantics. A DDM-protected relation
    can only be accessed via datasharing if DDM has been turned off for
    datasharing via: ALTER TABLE rel MASKING OFF FOR DATASHARES

    Note: The flag MASKING OFF FOR DATASHARES is checked in two locations.
            1) In command ALTER DATASHARE ds ADD TABLE
            2) When accessing the relation via datasharing
          The second check is required because DDM can be enabled on relations
          that are already in a datashare.
    '''

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_add_ddm_relation_to_datashare(self, cluster, db_session, relkind):
        '''
        Test that a DDM-protected relation (see relkind) can only be added to a
        datashare if flag 'is_masking_datashare_on' is set to false for that
        relation.
        '''
        with self.db_context(
                setup=[
                    fullpath('fixture_ddm_ds_prod_setup'),
                    fullpath('fixture_ddm_ds_prod_ddm_setup'),
                    fullpath('fixture_ddm_ds_prod_datashare_setup')
                ],
                cluster=cluster,
                session=db_session,
                format={
                    "rel_kind": REL_MAP[relkind],
                    "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                    "lbv_suffix": get_lbv_suffix(relkind),
                }
        ):
            self.execute_test_file(
                fullpath('test_add_ddm_rel_to_datashare'), session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_add_ddm_relation_to_datashare_feature_disabled(
            self, cluster, db_session, relkind):
        '''
        Test that a DDM-protected relation (see relkind) cannot be added to a
        datashare if GUC ddm_enable_for_datashares is OFF (feature disabled).
        '''
        with self.db_context(
                setup=[
                    fullpath('fixture_ddm_ds_prod_setup'),
                    fullpath('fixture_ddm_ds_prod_ddm_setup'),
                    fullpath('fixture_ddm_ds_prod_datashare_setup')
                ],
                cluster=cluster,
                session=db_session,
                format={
                    "rel_kind": REL_MAP[relkind],
                    "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                    "lbv_suffix": get_lbv_suffix(relkind),
                }
        ):
            cluster_session = ClusterSession(cluster)
            with cluster_session(gucs={"ddm_enable_for_datashares": "false"}):
                self.execute_test_file(
                    fullpath(
                        'test_add_ddm_rel_to_datashare_feature_disabled'
                    ),
                    session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_add_ddm_dependent_relation_to_datashare(self, cluster, db_session,
                                                     relkind):
        '''
        Test that a view (regular or LBV) that references a DDM-protected
        relation (see relkind) can only be added to a datashare if flag
        'is_masking_datashare_on' is set to false for the DDM-protected
        relation.

        Note: MVs currently cannot reference DDM-protected relations.
        '''
        with self.db_context(
            setup=[
                fullpath("fixture_ddm_ds_prod_setup"),
                fullpath("fixture_ddm_ds_prod_ddm_setup"),
                fullpath("fixture_ddm_ds_prod_datashare_setup"),
            ],
            cluster=cluster,
            session=db_session,
            format={
                "rel_kind": REL_MAP[relkind],
                "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                "lbv_suffix": get_lbv_suffix(relkind),
            }
        ):
            self.execute_test_file(
                fullpath('test_add_ddm_dependent_rel_to_datashare'),
                session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_add_ddm_dependent_relation_to_datashare_feature_disabled(
            self, cluster, db_session, relkind):
        '''
        Test that a view (regular or LBV) that references a DDM-protected
        relation (see relkind) cannot be added to a datashare if GUC
        ddm_enable_for_datashares is OFF (feature disabled).

        Note: MVs currently cannot reference DDM-protected relations.
        '''
        with self.db_context(
            setup=[
                fullpath("fixture_ddm_ds_prod_setup"),
                fullpath("fixture_ddm_ds_prod_ddm_setup"),
                fullpath("fixture_ddm_ds_prod_datashare_setup"),
            ],
            cluster=cluster,
            session=db_session,
            format={
                "rel_kind": REL_MAP[relkind],
                "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                "lbv_suffix": get_lbv_suffix(relkind),
            }
        ):
            cluster_session = ClusterSession(cluster)
            with cluster_session(gucs={"ddm_enable_for_datashares": "false"}):
                self.execute_test_file(
                    fullpath(
                        'test_add_ddm_dependent_rel_to_datashare_feature_disabled'
                    ),
                    session=db_session)

    @contextmanager
    def _setup_test_datashares(self, cluster, producer_db, share_name,
                               consumer_db_name, producer_cluster_id,
                               consumer_cluster_id, schema_name):
        with RedshiftDb(
               cluster.get_conn_params(db_name=producer_db)
             ) as prod_obj, \
             prod_obj.cursor() as cursor, \
             self.simulated_datashare(
                cluster=cluster,
                cursor=cursor,
                share_name=share_name,
                consumer_db_name=consumer_db_name,
                producer_cluster_id=producer_cluster_id,
                consumer_cluster_id=consumer_cluster_id,
             ):
            cursor.execute(
                'GRANT USAGE ON DATASHARE {} TO NAMESPACE \'{}\''.format(
                    share_name, consumer_cluster_id))
            cursor.execute('ALTER DATASHARE {} ADD SCHEMA {}'.format(
                share_name, schema_name))
            cursor.execute(
                'ALTER DATASHARE {} ADD ALL TABLES IN SCHEMA {}'.format(
                    share_name, schema_name))
            yield

    @pytest.mark.localhost_only  # Accessing catalog table, so need bootstrap.
    @pytest.mark.session_ctx(role='sys:secadmin')
    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_datasharing_query(self, cluster, cluster_session, db_session,
                               relkind):
        '''
        Test that a datasharing query on a DDM-protected relation (see relkind)
        or a view (regular or LBV) that refers to a DDM-protected relation
        will only execute when flag 'is_masking_datashare_on' is set to false
        on the DDM-protected relation.

        Note: For this test, in producer cluster we first add the relation
        to datashare and then attach masking policy on that relation. Because,
        with 'ddm_enable_for_datashares' GUC set, adding DDM protected relation
        to datashare is disabled.
        '''
        alter = get_alter_relkind(relkind)
        with self.db_context(  # Producer script before creating datashare.
            setup=[
                fullpath("fixture_ddm_ds_prod_setup"),
                fullpath("fixture_ddm_ds_prod_datashare_setup"),
            ],
            cluster=cluster,
            session=db_session,
            db_name="ddm_producer_db",
            format={
                "rel_kind": REL_MAP[relkind],
                "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                "lbv_suffix": get_lbv_suffix(relkind),
            }
        ), self.db_context(  # Consumer script creating external schema.
            setup=fullpath("fixture_consumer_setup"),
            cluster=cluster,
            session=db_session,
        ), self._setup_test_datashares(  # Setup simulated datashare.
            cluster,
            producer_db="ddm_producer_db",
            share_name="ddm_share",
            consumer_db_name="ddm_consumer_db",
            producer_cluster_id="ddm-prod-cluster-id",
            consumer_cluster_id="ddm-cons-cluster-id",
            schema_name="ddm_schema",
        ), self.db_context_existing_database(  # Producer post datashare setup.
            setup=fullpath("fixture_producer_{}_post_setup".format(alter)),
            cluster=cluster,
            session=db_session,
            db_name="ddm_producer_db",
        ):
            self.execute_test_file(
                fullpath('test_datasharing_query'), session=db_session)

    @pytest.mark.localhost_only  # Accessing catalog table, so need bootstrap.
    @pytest.mark.session_ctx(role='sys:secadmin')
    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_datasharing_query_feature_disabled(self, cluster, cluster_session,
                                                db_session, relkind):
        '''
        Test that if GUC 'ddm_enable_for_datashares' is OFF, a datasharing query
        on a DDM-protected relation (see relkind) or a view (regular or LBV)
        that refers to a DDM-protected relation does not execute.
        '''
        with cluster_session(gucs={'ddm_enable_for_datashares': 'false'}), \
            self.db_context(  # Producer script before creating datashare.
                setup=[
                    fullpath(
                        'fixture_ddm_ds_prod_feature_off_{}_setup'
                        .format(relkind)),
                    fullpath('fixture_ddm_ds_prod_datashare_setup')],
                cluster=cluster,
                session=db_session,
                db_name='ddm_producer_db'), \
            self.db_context(  # Consumer script creating external schema.
                setup=fullpath('fixture_consumer_setup'),
                cluster=cluster,
                session=db_session), \
            self._setup_test_datashares(  # Setup simulated datashare.
                 cluster,
                 producer_db='ddm_producer_db',
                 share_name='ddm_share',
                 consumer_db_name='ddm_consumer_db',
                 producer_cluster_id='ddm-prod-cluster-id',
                 consumer_cluster_id='ddm-cons-cluster-id',
                 schema_name='ddm_schema'), \
            self.db_context_existing_database(  # Producer post datashare setup.
                setup=fullpath('fixture_producer_feature_off_post_setup'),
                cluster=cluster,
                session=db_session,
                db_name='ddm_producer_db'):
            self.execute_test_file(
                fullpath('test_datasharing_query_feature_disabled'),
                session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    @pytest.mark.session_ctx(user_type="super")
    def test_alter_ddm_for_datashares_feature_on_superuser(self, db_session,
                                                           relkind):
        '''
        Test to validate ALTER DDM FOR DATASHARES is allowed for superuser
        when the feature is enabled.
        '''
        self.execute_templated_sql_test(
            fullpath("test_alter_ddm_for_datashares_feature_on"),
            dict(
                rel=REL_MAP[relkind],
                alt=REL_MAP[get_alter_relkind(relkind)],
                lbv=get_lbv_suffix(relkind),
            ),
            session=db_session,
        )

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    @pytest.mark.session_ctx(role='sys:secadmin')
    def test_alter_ddm_for_datashares_feature_on_secadmin(self, db_session,
                                                          relkind):
        '''
        Test to validate ALTER DDM FOR DATASHARES is allowed for secadmin
        when the feature is enabled.
        '''
        self.execute_templated_sql_test(
            fullpath('test_alter_ddm_for_datashares_feature_on'),
            dict(
                rel=REL_MAP[relkind],
                alt=REL_MAP[get_alter_relkind(relkind)],
                lbv=get_lbv_suffix(relkind),
            ),
            session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_alter_ddm_for_datashares_feature_on_reguser(self, db_session,
                                                         relkind):
        '''
        Test to validate ALTER DDM FOR DATASHARES is not allowed for reguser.
        '''
        self.execute_templated_sql_test(
            fullpath('test_alter_ddm_for_datashares_feature_on_reguser'),
            dict(
                rel=REL_MAP[relkind],
                alt=REL_MAP[get_alter_relkind(relkind)],
                lbv=get_lbv_suffix(relkind),
            ),
            session=db_session)

    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    @pytest.mark.session_ctx(role='sys:secadmin')
    def test_alter_ddm_for_datashares_feature_off(self, cluster_session,
                                                  db_session, relkind):
        '''
        Test to validate ALTER DDM FOR DATASHARES is not allowed when the
        feature is disabled.
        '''
        with cluster_session(gucs={'ddm_enable_alter_for_datashares': 'false'}):
            self.execute_templated_sql_test(
                fullpath('test_alter_ddm_for_datashares_feature_off'),
                dict(
                    rel=REL_MAP[relkind],
                    alt=REL_MAP[get_alter_relkind(relkind)],
                    lbv=get_lbv_suffix(relkind),
                ),
                session=db_session)

    def test_alter_ddm_for_datashares_view_off_reguser(
        self,
        cluster,
        db_session,
        cluster_session,
    ):
        '''
        Test to validate ALTER DDM FOR DATASHARES is not allowed for regular
        users when DDM on views/MVs features are disabled by GUC.

        Note: create testfile with test_alter_ddm_for_datashares_view_off.sql
        '''
        with self.db_context(
            setup=fullpath('fixture_alter_ddm_for_datashares_view_off_setup'),
            cluster=cluster,
            session=db_session
        ), cluster_session(gucs={
            'enable_ddm_on_mvs': 'false',
            'enable_ddm_on_views': 'false',
        }):
            self.execute_test_file(
                fullpath('test_alter_ddm_for_datashares_view_off_reguser'),
                session=db_session)

    @pytest.mark.session_ctx(role='sys:secadmin')
    def test_alter_ddm_for_datashares_view_off_secadmin(
        self,
        cluster,
        db_session,
        cluster_session,
    ):
        '''
        Test to validate ALTER DDM FOR DATASHARES is not allowed for secadmin
        when DDM on views/MVs features are disabled by GUC.

        Note: create testfile with test_alter_ddm_for_datashares_view_off.sql
        '''
        with self.db_context(
            setup=fullpath('fixture_alter_ddm_for_datashares_view_off_setup'),
            cluster=cluster,
            session=db_session
        ), cluster_session(gucs={
            'enable_ddm_on_mvs': 'false',
            'enable_ddm_on_views': 'false',
        }):
            self.execute_test_file(
                fullpath('test_alter_ddm_for_datashares_view_off_secadmin'),
                session=db_session)

    @pytest.mark.session_ctx(user_type="super")
    def test_alter_ddm_for_datashares_view_off_super(
        self,
        cluster,
        db_session,
        cluster_session,
    ):
        '''
        Test to validate ALTER DDM FOR DATASHARES is not allowed for super
        users when DDM on views/MVs features are disabled by GUC. Note super
        user can disable DDM for datashares even when the GUC is off.

        Note: create testfile with test_alter_ddm_for_datashares_view_off.sql
        '''
        with self.db_context(
            setup=fullpath('fixture_alter_ddm_for_datashares_view_off_setup'),
            cluster=cluster,
            session=db_session
        ), cluster_session(gucs={
            'enable_ddm_on_mvs': 'false',
            'enable_ddm_on_views': 'false',
        }):
            self.execute_test_file(
                fullpath('test_alter_ddm_for_datashares_view_off_super'),
                session=db_session)

    @pytest.mark.localhost_only  # Accessing catalog table, so need bootstrap.
    @pytest.mark.session_ctx(role='sys:secadmin')
    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_ddm_datasharing_access_downgrade(self, cluster, cluster_session,
                                              db_session, relkind):
        '''
        Test that, on downgrade,
        1. Datasharing queries on DDM relations will output in the same way, as
        on downgrade DDM on datashare is enabled: GUC ddm_enable_for_datashares
        is enabled.
        2. ALTER MASKING FOR DATASHARES will be blocked: GUC
        ddm_enable_alter_for_datashares is disabled on downgrade.
        3. ALTER ROW LEVEL SECURTY FOR DATASHARES is unaffected. On altering RLS
        flag values, queries should output as expected.

        Note: With new cluster session with a different GUC value, any existing
        cursor reference gets removed. simulated_datashare() needs to have
        access to the cursor it was initialized with during it finally step.
        Thus, currently we cannot initiate a cluster session after setting up
        the simulated datashare. In this test, we are creating the GUC disabled
        GUC disabled cluster session before the simulated datashare setup.
        '''
        alter = get_alter_relkind(relkind)
        with self.db_context(  # Producer script before creating datashare.
                setup=[
                    fullpath('fixture_ddm_ds_prod_setup'),
                    fullpath('fixture_ddm_ds_prod_datashare_setup')
                ],
                cluster=cluster,
                session=db_session,
                db_name='ddm_producer_db',
                format={
                    "rel_kind": REL_MAP[relkind],
                    "alter_kind": REL_MAP[get_alter_relkind(relkind)],
                    "lbv_suffix": get_lbv_suffix(relkind)
                }
            ), self.db_context(  # Consumer script creating external schema.
                setup=fullpath('fixture_consumer_setup'),
                cluster=cluster,
                session=db_session
            ), cluster_session(
                gucs={'ddm_enable_alter_for_datashares': 'false'}
            ), self._setup_test_datashares(  # Setup simulated datashare.
                 cluster,
                 producer_db='ddm_producer_db',
                 share_name='ddm_share',
                 consumer_db_name='ddm_consumer_db',
                 producer_cluster_id='ddm-prod-cluster-id',
                 consumer_cluster_id='ddm-cons-cluster-id',
                 schema_name='ddm_schema'
            ), self.db_context_existing_database(  # Producer post datashare setup.
                setup=fullpath('fixture_producer_{}_post_setup'.format(alter)),
                cluster=cluster,
                session=db_session,
                db_name='ddm_producer_db'
        ):

            # Test that consumer queries should output in the same way on
            # downgrade.
            self.execute_test_file(fullpath('test_datasharing_query'),
                                   session=db_session)

            # Test that alter table DDM is blocked and alter table rls is
            # unaffected.
            db_conn_params = cluster.get_conn_params(db_name='ddm_producer_db')
            test_file_path = fullpath(
                'test_ddm_datasharing_access_downgrade_producer_{}'.format(
                    alter))
            with RedshiftDb(db_conn_params) as db_prod_session:
                self.execute_test_file(test_file_path, session=db_prod_session)

            # Test that queries on relations after ALTER RLS on downgrade
            # outputs as expected.
            self.execute_test_file(
                fullpath('test_ddm_datasharing_access_downgrade_consumer'),
                session=db_session)

    @pytest.mark.session_ctx(role='sys:secadmin')
    @pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
    def test_svv_attached_masking_policy_ds_column(self, cluster, db_session,
                                                   relkind):
        '''
        TEST the SVV_ATTACH_MASKING_POLICY 'is_masking_datashare_on' column
        value on diffrent flag states: ON, OFF, DEFAULT for supported
        relkinds.
        '''
        self.execute_templated_sql_test(
            fullpath('test_svv_attached_masking_policy_ds_column'),
            dict(
                rel=REL_MAP[relkind],
                alt=REL_MAP[get_alter_relkind(relkind)],
                lbv=get_lbv_suffix(relkind),
            ),
            session=db_session)


@pytest.mark.localhost_only  # Datasharing SSM requirement
@pytest.mark.serial_only  # Datasharing SSM requirement
@pytest.mark.session_ctx(user_type='bootstrap')  # Datasharing SSM requirement
@pytest.mark.super_simulated_mode  # Datasharing SSM requirement
@pytest.mark.parametrize("relkind", ["table", "view", "lbv", "mv"])
class TestDDMDatasharingSSM(DDMTest, DatasharingTest):
    '''
    Test suite to validate DDM Datasharing upgrade, downgrade using Datasharing
    SSM.
    '''

    @contextmanager
    def setup_producer_consumer_in_ssm(self,
                                       cluster,
                                       ssm_consumer_cluster,
                                       imported_db_name,
                                       share_name):
        # We need to be a bootstrap user to access pg_datashare
        bu_ctx = SessionContext(user_type='bootstrap')
        su_ctx = SessionContext(user_type='super')
        consumer_conn_params = ssm_consumer_cluster.get_conn_params()
        producer_conn_params = cluster.get_conn_params()
        with DbSession(producer_conn_params, session_ctx=bu_ctx) as prod_session, \
                prod_session.cursor() as prod_cursor, \
                DbSession(consumer_conn_params, session_ctx=su_ctx) as cons_session, \
                cons_session.cursor() as cons_cursor, \
                ssm_datashare(
                    ssm_consumer_cluster, cluster, share_name, imported_db_name):
            yield [prod_cursor, cons_cursor]

    def rls_ds_on_prod_setup(self, producer_cursor, relkind):
        alter = REL_MAP[get_alter_relkind(relkind)]
        lbv = get_lbv_suffix(relkind)

        producer_cursor.execute("CREATE SCHEMA ddm_schema")
        producer_cursor.execute("CREATE TABLE ddm_schema.base_table (a int)")
        producer_cursor.execute(
            "INSERT INTO ddm_schema.base_table VALUES (1), (2), (3)")
        # RLS-for-datashare must be ON by default on RLS-enabled datasharing object.
        producer_cursor.execute(
            "CREATE {} ddm_schema.rls_on_rel AS SELECT * FROM ddm_schema.base_table {}"
            .format(REL_MAP[relkind], lbv))
        producer_cursor.execute(
            "ALTER {} ddm_schema.rls_on_rel ROW LEVEL SECURITY ON"
            .format(alter))
        # Enable RLS for datashares on RLS-enabled datasharing object.
        producer_cursor.execute(
            "CREATE {} ddm_schema.rls_for_ds_on_rel AS SELECT * FROM "
            "ddm_schema.base_table {}".format(REL_MAP[relkind], lbv))
        producer_cursor.execute(
            "ALTER {} ddm_schema.rls_for_ds_on_rel ROW LEVEL SECURITY "
            "ON FOR DATASHARES".format(alter))
        # Disable RLS for datashares on RLS-enabled datasharing object.
        producer_cursor.execute(
            "CREATE {} ddm_schema.rls_for_ds_off_rel AS SELECT * FROM "
            "ddm_schema.base_table {}".format(REL_MAP[relkind], lbv))
        producer_cursor.execute(
            "ALTER {} ddm_schema.rls_for_ds_off_rel ROW LEVEL SECURITY ON"
            .format(alter))
        producer_cursor.execute(
            "ALTER {} ddm_schema.rls_for_ds_off_rel ROW LEVEL SECURITY "
            "OFF FOR DATASHARES".format(alter))
        producer_cursor.execute(
            "ALTER DATASHARE simulated_share ADD SCHEMA ddm_schema")
        producer_cursor.execute(
            "ALTER DATASHARE simulated_share ADD ALL TABLES IN SCHEMA ddm_schema"
        )
        producer_cursor.execute(
            "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC TO DATASHARE simulated_share"
        )

    def verify_rls_prod(self, prod_cursor):
        prod_cursor.execute(
            "SELECT relname, is_rls_datashare_on from svv_rls_relation ORDER BY relname"
        )
        assert prod_cursor.fetchall() == [('rls_for_ds_off_rel', 'f'),
                                          ('rls_for_ds_on_rel', 't'),
                                          ('rls_on_rel', 't')]

    def verify_rls_consumer_ds_query(self, consumer_db_name, schema_name,
                                     consumer_db, consumer_cursor):
        error_message = "RLS-protected relation \"rls_on_rel\" cannot be "
        "accessed via datasharing query."
        error_message_2 = "RLS-protected relation \"rls_for_ds_on_rel\" cannot "
        "be accessed via datasharing query."

        self.assert_consumer_query_error(consumer_db_name, schema_name,
                                         "rls_on_rel", consumer_db,
                                         error_message)
        self.assert_consumer_query_error(consumer_db_name, schema_name,
                                         "rls_for_ds_on_rel", consumer_db,
                                         error_message_2)

        consumer_cursor.execute("SELECT * FROM {}.{}.{} ORDER BY 1".format(
            consumer_db_name, schema_name, "rls_for_ds_off_rel"))
        assert consumer_cursor.fetchall() == [(1, ), (2, ), (3, )]

    def test_ddm_datasharing_upgrade(self, cluster, ssm_consumer_cluster,
                                     relkind):
        """
        Verify that on upgrade with setting the DDM datasharing access control
        GUC 'ddm_enable_for_datashares' to ON:
        * Existing RLS datasharing access values are unaffected.
        * Query outputs on RLS relations with existing datasharing access values
        do not change.
        """
        with ClusterSession(cluster)(gucs=FEATURE_OFF), \
            ClusterSession(ssm_consumer_cluster)(gucs=FEATURE_OFF), \
                self.setup_producer_consumer_in_ssm(
                cluster, ssm_consumer_cluster, IMPORTED_DB_NAME, SHARE_NAME) \
                as cursors, DbSession(ssm_consumer_cluster.get_conn_params(),
                                      session_ctx=BU_CONTEXT) as cons_session, \
                cons_session.cursor() as cons_cursor:
            prod_cursor = cursors[0]

            self.rls_ds_on_prod_setup(prod_cursor, relkind)
            self.verify_rls_prod(prod_cursor)
            self.verify_rls_consumer_ds_query(IMPORTED_DB_NAME, "ddm_schema",
                                              cons_session, cons_cursor)

            with ClusterSession(cluster)(gucs=FEATURE_ON), \
                ClusterSession(ssm_consumer_cluster)(gucs=FEATURE_ON), \
                DbSession(ssm_consumer_cluster.get_conn_params(),
                          session_ctx=BU_CONTEXT) as cons_session_2, \
                DbSession(cluster.get_conn_params(), session_ctx=BU_CONTEXT) \
                    as prod_session_2, \
                    cons_session_2.cursor() as cons_cursor_2, \
                    prod_session_2.cursor() as prod_cursor_2:
                self.verify_rls_prod(prod_cursor_2)
                self.verify_rls_consumer_ds_query(IMPORTED_DB_NAME,
                                                  "ddm_schema", cons_session_2,
                                                  cons_cursor_2)
                prod_cursor_2.execute(
                    "ALTER DATASHARE simulated_share REMOVE SCHEMA ddm_schema")
                prod_cursor_2.execute(
                    "DROP SCHEMA IF EXISTS ddm_schema CASCADE")

    def test_ddm_datasharing_downgrade(self, cluster, ssm_consumer_cluster,
                                       relkind):
        """
        Verify that on downgrade with setting the DDM datasharing access control
        GUC 'ddm_enable_for_datashares' to OFF:
        * Existing RLS datasharing access values are unaffected.
        * Query outputs on RLS relations with existing datasharing access values
        do not change.
        """
        with ClusterSession(cluster)(gucs=FEATURE_ON), \
            ClusterSession(ssm_consumer_cluster)(gucs=FEATURE_ON), \
                self.setup_producer_consumer_in_ssm(
                cluster, ssm_consumer_cluster, IMPORTED_DB_NAME, SHARE_NAME) \
                as cursors, DbSession(ssm_consumer_cluster.get_conn_params(),
                                      session_ctx=BU_CONTEXT) as cons_session, \
                cons_session.cursor() as cons_cursor:
            prod_cursor = cursors[0]

            self.rls_ds_on_prod_setup(prod_cursor, relkind)
            self.verify_rls_prod(prod_cursor)
            self.verify_rls_consumer_ds_query(IMPORTED_DB_NAME, "ddm_schema",
                                              cons_session, cons_cursor)

            with ClusterSession(cluster)(gucs=FEATURE_OFF), \
                ClusterSession(ssm_consumer_cluster)(gucs=FEATURE_OFF), \
                DbSession(ssm_consumer_cluster.get_conn_params(),
                          session_ctx=BU_CONTEXT) as cons_session_2, \
                DbSession(cluster.get_conn_params(), session_ctx=BU_CONTEXT) \
                    as prod_session_2, \
                    cons_session_2.cursor() as cons_cursor_2, \
                    prod_session_2.cursor() as prod_cursor_2:
                self.verify_rls_prod(prod_cursor_2)
                self.verify_rls_consumer_ds_query(IMPORTED_DB_NAME,
                                                  "ddm_schema", cons_session_2,
                                                  cons_cursor_2)
                prod_cursor_2.execute(
                    "ALTER DATASHARE simulated_share REMOVE SCHEMA ddm_schema")
                prod_cursor_2.execute(
                    "DROP SCHEMA IF EXISTS ddm_schema CASCADE")
