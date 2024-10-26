# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import itertools

from test_burst_write_id_col_commit import BurstWriteIdentityColumnBase
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)


class BWIdColExtraMethods(BurstWriteIdentityColumnBase):
    """
    This class implements several general test functions for combination test
    for burst write on identity columns with other DDL cmds. The test functions
    cover running burst write on id cols interleaved with other DDLs within or
    without transaction blocks.
    """

    def _insert_identity_data_extra(self, cluster, cursor, iteration_num,
                                    main_tbl, burst_tbl, vector):
        """
        This method inserts data into the table and runs the extra method
        inside a committed transaction block with insert statements.
        """
        basic_insert = ("insert into {}(c0, c2) values({}, default);")
        basic_insert_2 = ("insert into {}(c0, c2) values({}, -10);")
        # Burst query
        cursor.execute("set query_group to burst;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Burst insert iteration {} {}".format(burst_tbl, i))
            cursor.execute(basic_insert.format(burst_tbl, i))
            cursor.execute(basic_insert_2.format(burst_tbl, i))
            # execute next extra method only after the 1st insert
            # because the afterward DMLs would not burst and no need to
            # execute again.
            if i == 0:
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)
        cursor.execute("commit;")
        # Normal query
        cursor.execute("set query_group to metrics;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Main insert iteration {} {}".format(main_tbl, i))
            cursor.execute(basic_insert.format(main_tbl, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(basic_insert_2.format(main_tbl, i))
            self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("commit;")

    def _insert_select_extra(self, cluster, cursor, iteration_num, main_tbl,
                             burst_tbl, vector):
        """
        This method inserts data into the table and runs the extra method
        inside a committed transaction block with insert select from statements
        """
        query_1 = ("insert into {}(c0) select c0 from {};")
        query_2 = ("insert into {}(c0, c2) select c0, c2 from {};")
        # Burst query
        cursor.execute("set query_group to burst;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Burst insert select iteration {} {}".format(
                burst_tbl, i))
            cursor.execute(query_1.format(burst_tbl, burst_tbl))
            cursor.execute(query_2.format(burst_tbl, burst_tbl))
            # execute next extra method after the 1st insert
            if i == 0:
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)
        cursor.execute("commit;")
        # Normal query
        cursor.execute("set query_group to metrics;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Main insert select iteration {} {}".format(main_tbl, i))
            cursor.execute(query_1.format(main_tbl, main_tbl))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(query_2.format(main_tbl, main_tbl))
            self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("commit;")

    def _insert_identity_data_abort_extra(self, cluster, cursor, iteration_num,
                                          main_tbl, burst_tbl, vector):
        """
        This method inserts data into the table and runs the extra method
        inside a aborted transaction block with insert select statements.
        """
        basic_insert_1 = ("insert into {}(c0, c2) values({}, default);")
        basic_insert_2 = ("insert into {}(c0, c2) values({}, -10);")
        # Normal query
        cursor.execute("set query_group to metrics;")
        for i in range(iteration_num):
            log.info("Main insert iteration {} {}".format(main_tbl, i))
            cursor.execute("begin;")
            cursor.execute(basic_insert_1.format(main_tbl, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(basic_insert_2.format(main_tbl, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            if i % 2 == 0:
                cursor.execute("commit;")
            else:
                cursor.execute("abort;")
        # Burst query
        prev_method = None
        cursor.execute("set query_group to burst;")
        for i in range(iteration_num):
            log.info("Burst insert iteration {} {}".format(burst_tbl, i))
            cursor.execute("begin;")
            cursor.execute(basic_insert_1.format(burst_tbl, i))
            # execute next extra method after the insert
            if prev_method is not None:
                # execute previous test method again if it was aborted
                prev_method(cluster, cursor, vector, main_tbl, burst_tbl)
            else:
                prev_method = self.exec_next_extra_method(
                    cluster, cursor, vector, main_tbl, burst_tbl)
            cursor.execute(basic_insert_2.format(burst_tbl, i))
            if i % 2 == 0:
                cursor.execute("commit;")
                prev_method = None
            else:
                cursor.execute("abort;")
                # Trigger local commit and backup for next burst after abort
                cursor.execute("set query_group to metrics;")
                cursor.execute("insert into dp31285_tbl_commit values(1);")
                self._start_and_wait_for_refresh(cluster)
                cursor.execute("set query_group to burst;")

    def _insert_select_abort_extra(self, cluster, cursor, iteration_num,
                                   main_tbl, burst_tbl, vector):
        """
        This method inserts data into the table and runs the extra method
        inside a aborted transaction block with insert select from statements.
        """
        insert_1 = ("insert into {}(c0) select c0 from {};")
        insert_2 = ("insert into {}(c0, c2) select c0, c2 from {};")
        # Normal query
        cursor.execute("set query_group to metrics;")
        for i in range(iteration_num):
            log.info("Main insert iteration {} {}".format(main_tbl, i))
            cursor.execute("begin;")
            cursor.execute(insert_1.format(main_tbl, main_tbl))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(insert_2.format(main_tbl, main_tbl))
            self._check_last_query_didnt_burst(cluster, cursor)
            if i % 2 == 0:
                cursor.execute("commit;")
            else:
                cursor.execute("abort;")
        # Burst query
        prev_method = None
        cursor.execute("set query_group to burst;")
        for i in range(iteration_num):
            log.info("Burst insert iteration {} {}".format(burst_tbl, i))
            cursor.execute("begin;")
            cursor.execute(insert_1.format(burst_tbl, burst_tbl))
            # execute next extra method after the insert
            if prev_method is not None:
                # execute previous test method again if it was aborted
                prev_method(cluster, cursor, vector, main_tbl, burst_tbl)
            else:
                prev_method = self.exec_next_extra_method(
                    cluster, cursor, vector, main_tbl, burst_tbl)
            cursor.execute(insert_2.format(burst_tbl, burst_tbl))
            if i % 2 == 0:
                cursor.execute("abort;")
                # Trigger local commit and backup for next burst after abort
                cursor.execute("set query_group to metrics;")
                cursor.execute("insert into dp31285_tbl_commit values(1);")
                self._start_and_wait_for_refresh(cluster)
                cursor.execute("set query_group to burst;")
            else:
                cursor.execute("commit;")
                prev_method = None

    def exec_next_extra_method(self, cluster, cursor, vector, main_tbl,
                               burst_tbl):
        """
        This method executes the extra method passed from the iterator
        `extra_test_methods_iter`. The method would be roundrobin infinitely
        from the method list.
        The extra methods can be other test cases including DDL.
        """
        next_extra_test_method = next(self.extra_test_methods_iter)
        next_extra_test_method(cluster, cursor, vector, main_tbl, burst_tbl)
        return next_extra_test_method

    def base_bw_id_cols_commit_extra_out_txn(self, cluster, vector,
                                             extra_test_methods,
                                             ssm_should_run_temp=False):
        """
        The general test method that can run any additional SQL statement with
        burst write on identity columns. The additional SQL statements can be
        specified by the list `extra_test_methods`.
        This method would run the extra test methods with committed burst write
        id col DMLs without transaction blocks.
        Test steps:
        1. Several rounds of burst DMLs to generate id cols values on burst LN
           and run extra test method.
        2. Validates id cols content.
        3. Several rounds of burst DMLs to generate id cols values on burst CN
           and run extra test method.
        4. Validates id cols content and does additional burst DMLs.
        """
        self.extra_test_methods_iter = itertools.cycle(extra_test_methods)
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector, ssm_should_run_temp)
            iterations = 2
            # test generate id values from LN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_identity_data(cluster, cursor, 2, main_tbl,
                                           burst_tbl)
                log.info("Begin validation {}".format(iter))
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)

                # execute next extra method after the insert
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)

                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            self._start_and_wait_for_refresh(cluster)
            self._insert_identity_data(cluster, cursor, 2, main_tbl, burst_tbl)
            log.info("Begin validation {}".format(iterations))
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

            # test generate id values from CN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_select(cluster, cursor, 2, main_tbl, burst_tbl)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)
                self._insert_select(cluster, cursor, 2, burst_tbl, main_tbl)

                # execute next extra method after the insert
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)

                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            self._start_and_wait_for_refresh(cluster)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)
            self._insert_select(cluster, cursor, 2, main_tbl, burst_tbl)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

    def base_bw_id_cols_abort_extra_out_txn(self, cluster, vector,
                                            extra_test_methods,
                                            ssm_should_run_temp=False):
        """
        The general test method that can run any additional SQL statement with
        burst write on identity columns. The additional SQL statements can be
        specified by the list `extra_test_methods`.
        This method would run the extra test methods with aborted burst write
        id col DMLs without transaction blocks.
        1. Several rounds of burst DMLs to generate id cols values on burst LN
           and run extra test method.
        2. Validates id cols content.
        3. Several rounds of burst DMLs to generate id cols values on burst CN
           and run extra test method.
        4. Validates id cols content and does additional burst DMLs.
        """
        self.extra_test_methods_iter = itertools.cycle(extra_test_methods)
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector, ssm_should_run_temp)
            iterations = 2
            # test generate id values from LN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_identity_data_abort(cluster, cursor, 2, main_tbl,
                                                 burst_tbl)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)

                # execute next extra method after the insert
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)

                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            # test generate id values from CN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_select_abort(cluster, cursor, 2, burst_tbl,
                                          main_tbl)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)

                # execute next extra method after the insert
                self.exec_next_extra_method(cluster, cursor, vector, main_tbl,
                                            burst_tbl)

                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            self._start_and_wait_for_refresh(cluster)
            self._insert_select_abort(cluster, cursor, 3, main_tbl, burst_tbl)
            self._validate_identity_column_data(cluster, cursor,
                                                vector.fix_slice)

    def base_bw_id_cols_commit_extra_in_txn(self, cluster, vector,
                                            extra_test_methods):
        """
        The general test method that can run any additional SQL statement with
        burst write on identity columns. The additional SQL statements can be
        specified by the list `extra_test_methods`.
        This method would run the extra test methods with committed burst write
        id col DMLs within transaction blocks.
        1. Several rounds of burst DMLs to generate id cols values on burst LN
           and run extra test method.
        2. Validates id cols content.
        3. Several rounds of burst DMLs to generate id cols values on burst CN
           and run extra test method.
        4. Validates id cols content and does additional burst DMLs.
        """
        self.extra_test_methods_iter = itertools.cycle(extra_test_methods)
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector)
            iterations = 3
            # test generate id values from LN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_identity_data_extra(cluster, cursor, 2, main_tbl,
                                                 burst_tbl, vector)
                log.info("Begin validation {}".format(iter))
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)
                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            # test generate id values from CN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_select_extra(cluster, cursor, 2, main_tbl,
                                          burst_tbl, vector)
                self._start_and_wait_for_refresh(cluster)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)
                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

    def base_bw_id_cols_abort_extra_in_txn(self, cluster, vector,
                                           extra_test_methods):
        """
        The general test method that can run any additional SQL statement with
        burst write on identity columns. The additional SQL statements can be
        specified by the list `extra_test_methods`.
        This method would run the extra test methods with aborted burst write
        id col DMLs within transaction blocks.
        1. Several rounds of burst DMLs to generate id cols values on burst LN
           and run extra test method.
        2. Validates id cols content.
        3. Several rounds of burst DMLs to generate id cols values on burst CN
           and run extra test method.
        4. Validates id cols content and does additional burst DMLs.
        """
        self.extra_test_methods_iter = itertools.cycle(extra_test_methods)
        db_session = DbSession(cluster.get_conn_params(user='master'))
        main_tbl = "dp31285_tbl"
        burst_tbl = "dp31285_tbl_burst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cluster, cursor, vector)
            iterations = 3
            # test generate id values from LN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_identity_data_abort_extra(
                    cluster, cursor, 2, main_tbl, burst_tbl, vector)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)
                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl

            # test generate id values from CN
            for iter in range(iterations):
                self._start_and_wait_for_refresh(cluster)
                self._insert_select_abort_extra(cluster, cursor, 2, main_tbl,
                                                burst_tbl, vector)
                self._validate_identity_column_data(cluster, cursor,
                                                    vector.fix_slice)
                # swap tables to run on main and burst cluster interleaved
                main_tbl, burst_tbl = burst_tbl, main_tbl
