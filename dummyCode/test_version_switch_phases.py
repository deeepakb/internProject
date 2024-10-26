# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
import json
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import (setup_teardown_burst, get_burst_cluster_arn,
                                   get_burst_cluster)
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]


@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
class TestVersionSwitchPhases(TestBurstWriteMVBase):
    """
    Test to verify entries in guc.StartupExcludePhases are not executed
    when the cluster version switch (restart reason = SwitchVersion)
    """

    def test_version_switch_phases(self, cluster):

        # Verifying that version switch occured
        burst_arn = get_burst_cluster_arn(cluster)
        query = '''
                select switch_time, switch_status
                from stl_burst_connection
                where cluster_arn = '{}'
                '''.format(burst_arn)

        lst = run_bootstrap_sql(cluster, query)
        switch_time, switch_status = lst[0]

        if switch_time == 0:
            pytest.skip("No Version Switch Occured")

        burst_cluster = get_burst_cluster(burst_arn)

        # Grabbing guc value before and after version switch to check that
        # appropriate phases were skipped
        query = '''
                select value
                from stl_guc
                where guc like '%startup_exclude_phases%'
                order by recordtime desc
                '''
        gucs = run_bootstrap_sql(burst_cluster, query)
        assert gucs, "startup_exclude_phases guc not implemented or has no value"

        post_switch_guc = json.loads(gucs[0][0])
        if len(gucs) == 2:
            pre_switch_guc = json.loads(gucs[1][0])
        else:
            pre_switch_guc = ''

        """
        To better understand the following blocks of code consider the following
        version switch timeline:

        Version switch (P171 preferred, VS to P172)
        1. cqi xstop on P171.       # old version of guc
        2. symlink change to P172
        3. cqi initdb on P172       # new version of guc
        4. cqi xstart on P172       # new version of guc

        In step 1, we will only skip shutdown phases specified in the old version
        of the guc.
        In step 3 and 4, we will only skip startup phases specifed in the new version
        of the guc

        This is why in the below code when we cycle through the pre version switch guc
        we only look at phases in the 'stop_padb' step. And then when we cycle through
        the post version switch guc we only look at phases not in the 'stop_padb' step.
        """

        # Verifying that phases were skipped while shutting down padb on
        # previous version
        if pre_switch_guc:
            for step in pre_switch_guc['SwitchVersion']:
                if step == "stop_padb":
                    for phase in pre_switch_guc['SwitchVersion'][step]:
                        query = '''
                                select count(*)
                                from stl_services_control_metrics
                                where phase = '{}'
                                and script_action = '{}'
                                and restart_reason = 'SwitchVersion'
                                '''.format(phase, step)
                        count = int(
                            run_bootstrap_sql(burst_cluster, query)[0][0])
                        assert count == 0, "Phase {} in {} not skipped" \
                            .format(phase, step)

        # Verifying that phases were skipped while starting padb on new version
        for step in post_switch_guc['SwitchVersion']:
            if step != "stop_padb":
                for phase in post_switch_guc['SwitchVersion'][step]:
                    query = '''
                            select count(*)
                            from stl_services_control_metrics
                            where phase = '{}'
                            and script_action = '{}'
                            and restart_reason = 'SwitchVersion'
                            '''.format(phase, step)
                    count = int(run_bootstrap_sql(burst_cluster, query)[0][0])
                    assert count == 0, "Phase {} in {} not skipped" \
                        .format(phase, step)
