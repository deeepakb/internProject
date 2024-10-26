# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
from datetime import datetime

import pytest

from raff.burst.burst_test import setup_teardown_burst, BurstTest

__all__ = [setup_teardown_burst]

log = logging.getLogger(__name__)

CUSTOM_AUTO_GUCS = {
        "try_burst_first": "false"
        }

_ELAPSE_TIMEOUT_SECONDS = 8.0


def freeze_queue(cluster, *args, **kwargs):
    return cluster.event('EtBurstFreezeQueueForTesting', *args, **kwargs)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstQueueFreezer(BurstTest):
    def execute_test_file_noburst(self, test_name, session=None):
        """
        Override parent method to set needed session settings for burst.
        """
        with session.cursor() as cursor:
            cursor.execute("set query_group to noburst")
            super(BurstTest, self).execute_test_file(test_name,
                                                     session=session)

    def test_queue_frozen(self, cluster, db_session):
        start_time = datetime.now()
        try:
            self.execute_test_file('burst_query', session=db_session)
        finally:
            elapsed_unblocked = datetime.now() - start_time
            log.info("Time difference when unblocked: {}"
                     .format(elapsed_unblocked.total_seconds()))
        with freeze_queue(cluster,
                          seconds=int(_ELAPSE_TIMEOUT_SECONDS),
                          group="burst"):
            start_time = datetime.now()
            try:
                self.execute_test_file('burst_query', session=db_session)
            finally:
                elapsed = datetime.now()-start_time
                log.info("Time difference when burst queue blocked: {}"
                         .format(elapsed.total_seconds()))
                if elapsed.total_seconds() < _ELAPSE_TIMEOUT_SECONDS:
                    pytest.fail("The queue control functions are "
                                "not working properly.")
            start_time = datetime.now()
            try:
                self.execute_test_file_noburst('burst_query',
                                               session=db_session)
            finally:
                elapsed = datetime.now()-start_time
                log.info("Time difference when burst queue blocked: {}"
                         .format(elapsed.total_seconds()))
                if elapsed.total_seconds() >= _ELAPSE_TIMEOUT_SECONDS:
                    pytest.fail("The queue control functions are "
                                "not working properly.")
        with freeze_queue(cluster, seconds=int(_ELAPSE_TIMEOUT_SECONDS)):
            start_time = datetime.now()
            try:
                self.execute_test_file('burst_query', session=db_session)
            finally:
                elapsed = datetime.now() - start_time
                log.info("Time difference when burst queue blocked: {}"
                         .format(elapsed.total_seconds()))
                if elapsed.total_seconds() < _ELAPSE_TIMEOUT_SECONDS:
                    pytest.fail("The queue control functions are "
                                "not working properly.")
            start_time = datetime.now()
            try:
                self.execute_test_file_noburst('burst_query',
                                               session=db_session)
            finally:
                elapsed = datetime.now() - start_time
                log.info("Time difference when burst queue blocked: {}"
                         .format(elapsed.total_seconds()))
                if elapsed.total_seconds() < _ELAPSE_TIMEOUT_SECONDS:
                    pytest.fail("The queue control functions are "
                                "not working properly.")
