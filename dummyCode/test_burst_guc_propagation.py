# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.common.db import get_random_identifier
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_test import (
        BurstTest,
        setup_teardown_burst,
        verify_query_bursted
    )
from raff.burst.burst_super_simulated_mode_helper import load_tpcds_data_tables

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]

log = logging.getLogger(__name__)

ALLOWED_GUCS = {
    "describe_field_name_in_uppercase": ["on", "off"],
    "spectrum_enable_dynamic_timeout": ["on", "off"],
    "spectrum_resolve_columns_by_name": ["on", "off"],
    "spectrum_enable_pseudo_columns": ["on", "off"],
    "enable_spectrum_recursive_scan": ["on", "off"],
    "enable_row_level_filtering": ["on", "off"],
    "skip_phase2_hashunique": ["on", "off"],
    "mmf_first_sk_only": ["on", "off"],
    "unload_byte_as_integer": ["on", "off"],
    "acceptinvchars_is_strict": ["on", "off"],
    "enable_arithmetic_expression_rrscan": ["on", "off"],
    "enable_numeric_rounding": ["on", "off"],
    "enable_float_round_half_even": ["on", "off"],
    "min_division_scale": ["0", "37"],
    "max_multiplication_scale": ["0", "37"],
    "error_on_nondeterministic_update": ["on", "off"],
    "enable_no_dist_keys_shuffling": ["on", "off"],
    "enable_mod_operator_fix": ["on", "off"],
    "exfunc_max_batch_rows": ["1", "2147483647"],
    "exfunc_max_batch_size": ["1", "5120"],
    "fallback_leader_node_random": ["on", "off"],
    "navigate_super_null_on_error": ["on", "off"],
    "parse_super_null_on_error": ["on", "off"],
    "cast_super_null_on_error": ["on", "off"],
    "json_parse_dedup_attributes": ["on", "off"],
    "json_parse_truncate_strings": ["on", "off"],
    "any_value_ignore_null": ["on", "off"],
    "super_allow_infinity": ["on", "off"],
    "default_geometry_encoding": ["1", "2"],
    "timezone":
    ["Etc/Greenwich", "America/New_York", "-8:00", "CST6CDT", "GB"],
    "datestyle": ["ISO, DMY", "Postgres, MDY", "SQL, YMD", "German, YMD"],
    "intervalstyle": ["postgres", "sql_standard", "postgres_verbose"],
    # Disabling this test, https://issues.amazon.com/issues/RedshiftDP-45213.
    # "extra_float_digits": ["-15", "2"],
    "statement_timeout": ["200000"],
    "application_name": ["bursttest"],
    "client_encoding": ["UNICODE", "UTF8"],
    "json_serialization_enable": ["on", "off"],
    # Cannot test readonly on with JDBC because session becomes readonly
    "readonly": ["on", "off"],
    "enable_spectrum_oid": ["on", "off"],
    "enable_exfunc_batching": ["on", "off"],
    "enable_exfunc_batching_on_new_queries": ["on", "off"],
    "enable_udf_batching_case_then_clause": ["on", "off"],
    "enable_exfunc_additional_hashjoin_batching": ["on", "off"],
    "exfunc_first_batch_rows": ["1", "10", "1000"],
    "exfunc_batch_multiplier": ["1", "100"],
    "small_table_block_threshold": ["1", "4", "8"],
    "selective_dispatch_level": ["1", "3", "6"],
    "selective_dispatch_timeout_ms": ["0", "10", "100", "1000"],
    "spectrum_force_json_parsing": ["on", "off"],
    "spectrum_enable_tolerant_json_parsing": ["on", "off"],
    "spectrum_pushdown_shorten": ["on", "off"],
    "spectrum_invalid_char_handling":
    ["DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW", "REPLACE"],
    "spectrum_surplus_char_handling":
    ["DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW", "TRUNCATE"],
    "spectrum_numeric_overflow_handling": [
        "DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW"
    ],
    "spectrum_type_coercion_failure_handling": [
        "DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW"
    ],
    "spectrum_surplus_bytes_handling": [
        "DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW"
    ],
    "spectrum_column_count_mismatch_handling": [
        "DISABLED", "FAIL", "SET_TO_NULL", "DROP_ROW"
    ],
    "spectrum_query_maxerror": ["-1", "65535"],
    "spectrum_query_maxerror": ["-1", "10000"],
    "spectrum_enable_dump_catalog_response_on_error": ["on", "off"],
    "spectrum_use_column_stats_aliases": ["on", "off"],
    "spectrum_use_column_stats_accurate": ["on", "off"],
    "spectrum_nested_as_super": ["on", "off"],
    "spectrum_max_logged_errors_per_scan": ["0", "100"],
    "spectrum_replacement_char": ["$"],
}

NOT_ALLOWED_GUCS = {
    "geqo": ["on", "off"],
    "try_pg_planner_budget": ["on", "off"],
    "default_transaction_read_only": ["on", "off"],
    "transaction_read_only": ["on", "off"],
    "enable_ctas_hop": ["on", "off"],
    "enable_reg_ctas_hop": ["on", "off"],
    "disable_sqa_in_session": ["on", "off"],
    "skip_count_for_ctas": ["on", "off"],
    "use_qdesc_for_vchar_zero": ["on", "off"],
    "enable_simple_query_stmt_cleanup": ["on", "off"],
    "spectrum_enable_search_path": ["on", "off"],
    "enable_setinstance_remap_skipping": ["on", "off"],
    "display_schema_in_explain": ["on", "off"],
    "enable_result_cache_for_session": ["on", "off"],
    "enable_result_cache_for_cursor": ["on", "off"],
    "use_resdom_in_append": ["on", "off"],
    "use_resdom_in_setop": ["on", "off"],
    "use_resdom_in_agg": ["on", "off"],
    "volt_optimize_child_cse": ["on", "off"],
    "volt_optimize_setop_child_cse_only": ["on", "off"],
    "volt_disallow_oid_in_cse": ["on", "off"],
    "wlm_query_slot_count": ["3", "6", "9"],
    "small_table_row_threshold": ["10", "100", "1000"],
    "vacuum_auto_wlm_user_queue_max_occ_percent": ["10", "20"],
    "analyze_threshold_percent": ["10", "20"],
    "default_transaction_isolation": ["serializable", "read committed"],
    "transaction_isolation": ["serializable", "read committed"],
}

ALLOWED_GUCS_PARAMS = [(guc, value)
                       for guc in ALLOWED_GUCS
                       for value in ALLOWED_GUCS[guc]]

NOT_ALLOWED_GUCS_PARAMS = [(guc, value)
                           for guc in NOT_ALLOWED_GUCS
                           for value in NOT_ALLOWED_GUCS[guc]]

CUSTOM_SETUP_GUCS = {
    'enable_burst_failure_handling': 'false',
    'enable_session_context_variable': 'true',
    'burst_disabled_functions': ''
}
CUSTOM_SETUP_GUCS_WITH_DISABLEMENT = {'enable_burst_failure_handling': 'false'}


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SETUP_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstGucPropagation(BurstTest):

    @pytest.mark.parametrize("name,new_val", ALLOWED_GUCS_PARAMS)
    def test_guc_propagated(self, name, new_val,
                            db_session, verify_query_bursted):
        with db_session.cursor() as cursor:
            cursor.execute("set {} to '{}';".format(name, new_val))
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('{}'),"
                           "count(*) from catalog_sales A;".format(name))
            row = cursor.fetchone()
            assert row[0] == new_val

    # TODO(nickzhu) DP-51521: re-emable JDBC test
    @pytest.mark.no_jdbc
    @pytest.mark.parametrize("name,new_val", ALLOWED_GUCS_PARAMS)
    def test_guc_propagated_with_alter_user(self, name, new_val, cluster,
                                            db_session, verify_query_bursted):
        db_session._establish_session_resources()
        reg_user_name = db_session.session_ctx.username
        super_ctx = SessionContext(user_type='super')
        super_session = DbSession(cluster.get_conn_params(),
                                  session_ctx=super_ctx)
        with super_session.cursor() as cursor:
            cursor.execute("ALTER USER {} SET {} TO '{}'".format(
                reg_user_name, name, new_val))

        with db_session.cursor() as cursor:
            cursor.execute("reset {};".format(name))
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('{}'),"
                           "count(*) from catalog_sales A;".format(name))
            row = cursor.fetchone()
            assert row[0].lower() == new_val.lower()

    @pytest.mark.parametrize("name,new_val", NOT_ALLOWED_GUCS_PARAMS)
    def test_guc_not_propagated(self, name, new_val,
                                db_session, verify_query_bursted):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('{}'),"
                           "count(*) from catalog_sales A;".format(name))
            row = cursor.fetchone()
            old_val = row[0]
            if old_val != new_val:
                cursor.execute("set {} to '{}';".format(name, new_val))
                cursor.execute("select current_setting('{}'),"
                               "count(*) from catalog_sales A;".format(name))
                row = cursor.fetchone()
                assert row[0] != new_val

    def test_query_group_not_propagated(
            self, db_session, verify_query_bursted):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('query_group'),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            assert row[0] != "burst"

    @pytest.mark.session_ctx(user_type='bootstrap')
    def test_session_authorization_not_propagated(
            self, db_session, verify_query_bursted):
        reguser = get_random_identifier(prefix_str='reg_', max_len=10)
        with db_session.cursor() as cursor:
            cursor.execute(
                "create user {} with password 'Testing1234'".format(reguser))
            cursor.execute("set session_authorization to {};".format(reguser))
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('session_authorization'),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            assert row[0] != reguser

    @pytest.mark.session_ctx(user_type='bootstrap')
    def test_search_path_not_propagated(
            self, db_session, verify_query_bursted):
        reguser = get_random_identifier(prefix_str='reg_', max_len=10)
        schema = get_random_identifier(prefix_str='schema_', max_len=10)
        with db_session.cursor() as cursor:
            cursor.execute(
                "create user {} with password 'Testing1234'".format(reguser))
            cursor.execute(
                "create schema {}".format(schema))
            cursor.execute(
                "grant all on schema {} to {}".format(schema, reguser))
            cursor.execute("set session_authorization to {};".format(reguser))
            cursor.execute("set query_group to burst;")
            cursor.execute("set search_path to '$user', public, {};".format(
                schema))
            cursor.execute("select current_setting('search_path'),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            assert row[0] != '"$user", public, {}'.format(schema)

    def test_session_context_variable_propagated(self, db_session,
                                                 verify_query_bursted):
        # Test scenario:
        # Set a session context variable on main cluster,
        # the variable will be propagated to burst cluster.
        with db_session.cursor() as cursor:
            cursor.execute("set context.custom_id to 123;")
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('context.custom_id'),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            assert row[0] == "123"

    def test_session_context_variable_propagated_if_null(
            self, db_session, verify_query_bursted):
        # Test scenario:
        # If the value of a session context variable is NULL on main cluster,
        # It can also be propagated to burst.
        with db_session.cursor() as cursor:
            # Set the value of context.custom_id to NULL in current session.
            cursor.execute(
                "select set_config('context.custom_id', NULL, false);")
            cursor.execute("set query_group to burst;")
            cursor.execute("select current_setting('context.custom_id'),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            assert row[0] is None


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SETUP_GUCS_WITH_DISABLEMENT,
                               initdb_before=True, initdb_after=True)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstGucPropagationWithDisablement(BurstTest):
    @pytest.mark.parametrize("seed_val", [0.1, 0.5, 1])
    def test_seed_not_propagated(
            self, seed_val, db_session, cluster):
        # random function is no longer allowed to run on burst cluster.
        with db_session.cursor() as cursor:
            load_tpcds_data_tables(cluster, 'catalog_sales')
            cursor.execute("set seed to {};".format(seed_val))
            cursor.execute("set query_group to burst;")
            cursor.execute("select random(),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            first_val = row[0]
            # Check query status for:
            # 18: Concurrency Scaling ineligible query - Function not supported
            self.check_last_query_didnt_burst_status(cluster, cursor, 18)
            cursor.execute("set seed to {};".format(seed_val))
            cursor.execute("set query_group to burst;")
            cursor.execute("select random(),"
                           "count(*) from catalog_sales A;")
            row = cursor.fetchone()
            second_val = row[0]
            # Check query status for:
            # 18: Concurrency Scaling ineligible query - Function not supported
            self.check_last_query_didnt_burst_status(cluster, cursor, 18)
            assert first_val == second_val
