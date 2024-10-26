# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import traceback
import uuid
import pytest
from os.path import join, basename
import plumbum
from contextlib import contextmanager
import hashlib
import re

from raff.common.ssh_user import SSHUser

from raff.common.db.db_exception import DatabaseError
from raff.burst.burst_test import BurstTest
from raff.common.aws_clients.s3_client import S3Client
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.common.remote_helper import SshFileHelper
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.util.utils import run_bootstrap_sql
from raff.monitoring.monitoring_test import MonitoringTestSuite
from plumbum import ProcessExecutionError

log = logging.getLogger(__name__)

# The guc that specifies the bucket used to store the stl files on S3 for the
# monitoring purposes.
S3_STL_BUCKET_GUC_KEY = 's3_stl_bucket'

# The guc that stores the backup prefix that is pushed to a cluster by Control
# Plane and monitoring uses the same to identify a unique S3 path for a primary
# cluster that has burst enabled.
S3_BACKUP_PREFIX_GUC_KEY = 's3_backup_key_prefix'

# The guc that contains the unique numeric id assigned to a cluster by the
# Control Plane.
CLUSTER_ID_GUC_KEY = 'cluster_id'

# The guc that contains the aws account id of the cluster
CLUSTER_OWNER_AWS_ACCT_ID_GUC_KEY = 'cluster_owner_aws_acct_id'

# The bucket we use for uploading the stl files for burst monitoring in the QA
# region.
MONITORING_S3_BUCKET_NAME = 'redshift-monitoring-qa'


@pytest.mark.cluster_only
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstStbQuerySecurity(BurstTest):
    '''
    This tests the security for the stcss. As part of burst, the monitoring
    connector uploads some selected stl files onto S3. As all these files, from
    all the different clusters land on the same S3 bucket, we want to make sure
    that its impossible for one cluster to access another clusters data. This
    security is provided by the instance profile of the cluster. A cluster is
    only allowed to access objects with prefix that matches the string as in
    the s3_backup_key_prefix guc. That's the first layer of security. Each
    object is also tagged with the key-value pair PrimaryClusterId=<cluster id>
    and each cluster can only query the objects where the value of the tag
    matches its own cluster id. Note that in order to do this testing we don't
    need to acquire burst clusters because the query against stcs can only be
    done from the primary cluster and presence or the absence of burst cluster
    affects it in no way but certainly delays the tests.
    '''

    def get_current_iam_roles(self, cluster):
        '''
        Gets the arns for the IAM Roles attached to the cluster.
        '''

        test_cluster_id = cluster.cluster_dict['ClusterIdentifier']
        rs_client = RedshiftClient(
            profile=cluster.profile, region=cluster.region)
        response = rs_client.client.describe_clusters(
            ClusterIdentifier=test_cluster_id)
        iam_roles_map = response['Clusters'][0]['IamRoles']
        iam_roles = []
        for role in iam_roles_map:
            iam_roles.append(role['IamRoleArn'])
        return iam_roles

    @contextmanager
    def remove_raff_specific_iam_roles(self, cluster):
        '''
        Raff tests do read from and write data to S3 and therefore a Redshift
        cluster being used for cluster based raff tests will have this two
        roles attached to them 'arn:aws:iam::467896856988:role/Redshift-S3' and
        'arn:aws:iam::467896856988:role/Redshift-S3-Write'. These roles give
        more access than we would like for the security tests. In fact for the
        security tests we want the cluster to be exactly as CP creates for us
        and no special permissions. Therefore, this helper method removes the
        roles if attached to the cluster. And then makes sure that they are
        added back later on.
        '''

        rs_client = RedshiftClient(
            profile=cluster.profile, region=cluster.region)

        iam_roles = self.get_current_iam_roles(cluster)
        if len(iam_roles) > 0:
            log.info("Removing iam roles {}".format(iam_roles))
            response = rs_client.client.modify_cluster_iam_roles(
                ClusterIdentifier=cluster.cluster_dict['ClusterIdentifier'],
                RemoveIamRoles=iam_roles)
            log.info("Response for removing iam roles ({}) : ".format(
                iam_roles, response))
            cluster.wait_till_no_workflows()
            # Check to see if the roles were really removed
            assert len(self.get_current_iam_roles(cluster)) == 0, (
                "{} roles "
                "could not be removed".format(iam_roles))
        else:
            log.info(
                "The cluster does not have any iam roles attached to it..")
        yield
        if len(iam_roles) > 0:
            log.info("Adding iam roles {}".format(iam_roles))
            response = rs_client.client.modify_cluster_iam_roles(
                ClusterIdentifier=cluster.cluster_dict['ClusterIdentifier'],
                AddIamRoles=iam_roles)
            log.info("Response for adding iam roles ({}): {}".format(
                iam_roles, response))
            cluster.wait_till_no_workflows()
            # Check to see if the roles were really removed
            assert len(
                self.get_current_iam_roles(cluster)) == len(iam_roles), (
                    "{} roles could not be added".format(iam_roles))

    def get_files_to_upload(self):
        '''
        This just gathers the full paths of the two files that will be uploaded
        to S3.
        '''

        current_path = str(plumbum.local.cwd)
        current_path = current_path + "/monitoring"
        data_path = current_path + "/data"
        self.file_names = [
            "CN0_stl_aggr_0_query.593301380770679.gz",
            "LN_stl_aggr_6411_padbmaster.593301380791355.gz"
        ]
        file_paths = []
        for f in self.file_names:
            file_paths.append(join(data_path, f))
        return file_paths

    def upload_files_to_other_cluster_STL_s3_from_leader(self, cluster):
        '''
        Run a command from the leader node of the redshift cluster to try to
        upload some files onto a S3 path. This path is a STL s3 path owned by
        another customer's cluster, so we expect access denied exception
        '''

        s3_bucket = cluster.get_padb_conf_value(S3_STL_BUCKET_GUC_KEY)
        my_cluster_id = cluster.get_padb_conf_value('immutable_cluster_id')
        my_account_id = cluster.get_padb_conf_value(
                    CLUSTER_OWNER_AWS_ACCT_ID_GUC_KEY)
        other_cluster_prefix = self.find_STL_prefix_owned_by_other_cluster(
            MONITORING_S3_BUCKET_NAME, my_cluster_id, my_account_id)
        log.info('prefix: {}'.format(other_cluster_prefix))

        with cluster.get_leader_ssh_conn() as sshc:
            for f in self.on_cluster_files:
                upload_to_s3_cmd = "aws s3 cp {} s3://{}/{} ".format(
                        f, s3_bucket, other_cluster_prefix)
                log.info('Running command ({})'.format(upload_to_s3_cmd))
                rc, stdout, stderr = sshc.run_remote_cmd(
                    upload_to_s3_cmd, user=SSHUser.RDSDB)

    def test_cannot_upload_objects_to_STL_s3_owned_by_other_cluster(
            self, cluster, db_session, cluster_session):
        '''
        When a cluster is created, the control plane pushes the instance
        profiles into the nodes that makes the monitoring connector able to
        archive the stl logs into the s3 bucket specified by the
        s3_stl_bucket guc and with the prefix specified by the
        s3_backup_key_prefix. This test aspires to test the negative scenario
        that is brought upon by the case, when the monitoring connector is
        trying to archive things to S3. Ideally, this should fail and S3 should
        throw us an access denied error.
        1. Set up a raff test cluster, this is automatically done by the raff
           framework.
        2. Remove the raff specific roles attached to the cluster. This is
           because the raff roles are much liberal and does not track the real
           world cluster security privileges granted.
        3. Upload local files to the redshift cluster.
        4. Use the on-cluster s3 client to upload cluster files to s3.
        5. Check that an exception is thrown.
        '''

        access_denied = False
        error_received = ""
        guc = {S3_STL_BUCKET_GUC_KEY: MONITORING_S3_BUCKET_NAME}
        with cluster_session(gucs=guc):
            self.on_cluster_files = []
            with SshFileHelper(
                    host=cluster.leader_public_ip,
                    region=cluster.region) as uploader:
                for f in self.get_files_to_upload():
                    on_cluster_file = join("/tmp", basename(f))
                    self.on_cluster_files.append(on_cluster_file)
                    # Upload the files from the local test machine to RS
                    # cluster
                    uploader.upload(f, on_cluster_file)
                with self.remove_raff_specific_iam_roles(cluster):
                    try:
                        # Now we attempt to upload the files from the cluster
                        # to S3, to assert our security settings
                        self.upload_files_to_other_cluster_STL_s3_from_leader(
                                cluster)
                    except ProcessExecutionError as e:
                        log.error("Got error: {}".format(str(e)))
                        if "(AccessDenied)" in str(e):
                            access_denied = True
                            error_received = str(e)
                    except Exception:
                        log.error(traceback.format_exc())
                        error_received = traceback.format_exc()
        # Its important that the re-attachment of IAM roles happen whether the
        # test passes or fails. Therefore, we assert here if an error occurred
        # and not immediately when we spotted it.
        assert access_denied, ("Access denied expected but found ({})".format(
            error_received))

    def find_STL_prefix_owned_by_other_cluster(
            self, monitoring_bucket, my_cluster_id, my_account_id):
        '''
        Try to find an existing S3 prefix from STL s3 path in monitoring_bucket
        is not owned by my_cluster_id. Throw exception when no valid STL path
        found.
        STL S3 path partition format: https://quip-amazon.com/c6AaA7tOEAVN
        S3://<bucket_name>/<sha256(awsaccountid)>/<awsaccountid>/<stl_name>/
        <immutable_cluster_id>/<date>/<cluster_type>/<physical_cluster_id>/raw/*

        Return example:
        03874aa5/943721087315/stl_aggr/fb2d90ed-501e-49d0-8797/

        Implementation details:
        We can not list all objects in the bucket to find the right prefix,
        because there are too many objects in this bucket with prefix
        <hash(account)>/qa. The prefix of these objects does not match STL
        prefix, it will cause a lot useless calls of list_objects_v2().

        This function calls match_s3_prefix() which will recursively
        match STL s3 partition pattern layer by layer, and return the first
        valid s3 prefix
        '''

        s3_client = S3Client(profile=Profiles.COOKIE_QA_GAUNTLET
            , region=Regions.IAD)
        account_hash = hashlib.sha256(
            my_account_id.encode("utf-8")).hexdigest()[:8]
        '''
        Assumption:
        Assume <vc_id> = <physical_cluster_id> in STL s3 path, <vc_id> might
        different with <physical_cluster_id> when cluster pause-and-resume.

        The prefix match process match to vc_id
        <sha256(awsaccountid)>/<awsaccountid>/<stl_name>/<immutable_cluster_id>/

        STL path patterns for each layer as below
        1. Exclude account_id since the IAM policy can be allow full acess to
        the account_id level.
        e.g. arn:aws:s3:::redshift-monitoring-qa/eb3116d6/837661609862/*
        2. Exclude cluster_id to find prefix owned by other cluster
        3. Match STL path stl_xxx
        '''

        patterns = [
                # exclude <hash(account_id)> -> 03874aa5
                r'(?!' + account_hash + ')[0-9a-zA-Z]{8}/'
                # exclude <account_id> -> 943721087315
                , r'(?!' + str(my_account_id) + ')[0-9]{12}/'
                # match <stl_xxx> -> stl_aggr
                , r'stl_[a-zA-Z]+/'
                # exlcude <immutable_cluster_id> -> fb2d90ed-501e-49d0-8797
                , r'(?!' + str(my_cluster_id) + ')[0-9a-f-]{1,36}/']

        # Current prefix and pattern are "",
        prefix = self.match_s3_prefix(s3_client, monitoring_bucket, patterns)

        # Throw exception to fail the test when no valid S3 prefix found. This
        # should rarely happen since we have enough cluster in QA to provide
        # a valid S3 prefix. You can resolve no valid prefix found issue by
        # creating a new cluster in the same account, then rerun the test.
        if not prefix:
            raise Exception("No valid STL prefix found in S3")
        return prefix

    def match_s3_prefix(self, s3_client, bucket_name, patterns):
        return self.dfs_match_s3_prefix(s3_client, bucket_name, patterns, 0
                , '', r'')

    def dfs_match_s3_prefix(self, s3_client, bucket_name, patterns
            , current_layer, current_prefix, prefix_pattern):
        '''
        Recursively match s3 prefix pattern for each layer, layers are separated
        by "/". Return the first matched prefix. Return None if not found any
        valid prefix.
        '''

        # Return prefix when all matched
        if current_layer == len(patterns):
            return current_prefix

        objects = s3_client.list_objects_v2(Bucket=bucket_name
                , Prefix=current_prefix, Delimiter='/' )
        current_pattern = prefix_pattern + patterns[current_layer]

        for obj in objects.get('CommonPrefixes', []):
            child_prefix = obj.get('Prefix')
            if not child_prefix or not re.findall(r'^' + current_pattern + r'$'
                , child_prefix):
                continue

            # Proceed to next layer when match current pattern
            prefix = self.dfs_match_s3_prefix(s3_client, bucket_name, patterns
                    , current_layer + 1, child_prefix, current_pattern)
            if prefix:
                return prefix
        return None

    @contextmanager
    def from_cluster_put_objects_onS3_with_tags(self, cluster, s3_prefix):
        '''
        Upload some files onto a S3 bucket path, that are tagged with something
        other than the expected tag that this cluster is allowed to query, for
        while running a query against the burst cluster logs.
        '''

        # which S3 bucket to upload files to
        s3_bucket = cluster.get_padb_conf_value(S3_STL_BUCKET_GUC_KEY)

        # The correct tag to use is the cluster id but we want to see that an
        # object tagged with something else does not get queried.
        # Correct tag value: cluster.get_padb_conf_value(CLUSTER_ID_GUC_KEY).
        # Change to right tag value should make the test fail.
        tag_value = 'incorrect_tag'
        s3_files = []
        s3 = S3Client(profile=Profiles.COOKIE_QA_GAUNTLET, region=Regions.IAD)
        with cluster.get_leader_ssh_conn() as sshc:
            for on_cluster_path in self.on_cluster_files:
                f_name = basename(on_cluster_path)
                full_s3_dest = s3_prefix + f_name
                try:
                    s3.put_object(
                        Body=on_cluster_path,
                        Bucket=s3_bucket,
                        Key=full_s3_dest,
                    )

                    s3.put_object_tagging(Bucket=s3_bucket,
                                        Key=full_s3_dest,
                                        Tagging={'TagSet':[{'Key':'PrimaryClusterID', 'Value':tag_value}]}
                                        )
                except Exception as ex:
                    log.info('Upload log with tagging failed with ex: %s ' % str(ex))
                    raise

                # check if the file was truly uploaded on S3
                tag_resp = s3.get_object_tagging(
                    Bucket=s3_bucket, Key=full_s3_dest)['TagSet'][0]
                log.info("The tag set for the cluster: {}".format(tag_resp))
                assert tag_value == tag_resp['Value']
                assert 'PrimaryClusterID' == tag_resp['Key']
                s3_files.append(full_s3_dest)
            yield

            # cleanup:  Delete the files from the S3 bucket.
            for s3_obj in s3_files:
                delete_resp = s3.delete_object(Bucket=s3_bucket, Key=s3_obj)
                assert delete_resp, ("Delete obj {} didn't return response {}",
                                     s3_obj, delete_resp)
                log.info("Delete obj ({}) response: {}".format(
                    s3_obj, delete_resp))
                assert delete_resp['DeleteMarker'] is True, (
                    "Could not delete {}".format(s3_obj))

    def test_only_objects_tagged_with_cluster_id_are_queryable(
            self, cluster, cluster_session):
        '''
        When the monitoring connector uploads objects to S3 it tags them with
        the cluster identifier and the instance profile of the object has the
        rule that a cluster can only access objects tagged by its own cluster
        id. This test verifies this assumption.
        1. Upload files on the cluster from the box running the tests.
        2. upload these objects from the cluster onto S3 with the prefix same
           as the back_key_prefix with the log name added to it and also with
           a tag that is other than the cluster id.
        3. Now try querying the stcs log and one should expect to get no result
           back as none of the objects are tagged with the required tag.
        '''
        guc = {S3_STL_BUCKET_GUC_KEY: MONITORING_S3_BUCKET_NAME,
               "enable_monitoring_connector_main_cluster": "false"}
        log.info("Changing gucs on the cluster: {}".format(guc))
        required_error_found = True
        error_received = ""
        with cluster_session(gucs=guc):
            self.on_cluster_files = []
            with SshFileHelper(
                    host=cluster.leader_public_ip,
                    region=cluster.region) as uploader:
                for f in self.get_files_to_upload():
                    on_cluster_file = join("/tmp", basename(f))
                    self.on_cluster_files.append(on_cluster_file)
                    # Upload the files from the local test machine to RS
                    # cluster
                    uploader.upload(f, on_cluster_file)

                # Construct the s3_prefix for uploading and stcs query.
                account_id = cluster.get_padb_conf_value(
                    CLUSTER_OWNER_AWS_ACCT_ID_GUC_KEY)
                account_id_sha256 = hashlib.sha256(
                    account_id.encode("utf-8")).hexdigest()[:8]
                cluster_id = cluster.get_padb_conf_value(CLUSTER_ID_GUC_KEY)
                immutable_cluster_id = \
                    cluster.get_padb_conf_value("immutable_cluster_id")
                cluster_id_in_partition =\
                    MonitoringTestSuite.get_stl_cluster_identifier_partition(
                        cluster, immutable_cluster_id, cluster_id)
                s3_prefix = '{}/{}/{}/{}/20200101/cs/{}/raw/'.format(
                    account_id_sha256, account_id, 'stl_aggr',
                    cluster_id_in_partition,
                    cluster_id)
                log.info("S3 prefix is: {}".format(s3_prefix))

                # Now we attempt to upload the files from the cluster to S3, to
                # assert our security settings
                with self.from_cluster_put_objects_onS3_with_tags(cluster, s3_prefix):
                    with self.remove_raff_specific_iam_roles(cluster):
                        # Now that we know that some files are there on the,
                        # bucket we want to query them.
                        sql = ("select count(*) from stcs_aggr "
                               "where __cluster_type = \'cs\' "
                               "and __path like \'%{}%\'".format(s3_prefix))
                        try:
                            log.info("Running query: {}".format(sql))
                            res = run_bootstrap_sql(cluster, sql)
                            row_count = res[0][0]
                            assert int(row_count) == 0
                        except ProcessExecutionError as e:
                            log.error("Got exception: {}".format(str(e)))
                            if "Spectrum Scan Error" not in str(e):
                                required_error_found = False
                                error_received = str(e)
                        except Exception:
                            required_error_found = False
                            log.error(traceback.format_exc())
                            error_received = traceback.format_exc()

        # Its important that the re-attachment of IAM roles happen whether the
        # test passes or fails. Therefore, we assert here if an error occurred
        # and not immediately when we spotted it.
        if not required_error_found:
            assert False, (
                "Looking for S3 exception, found: ({})".format(error_received))
