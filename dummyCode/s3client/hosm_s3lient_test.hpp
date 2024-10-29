// Copyright 2021 Amazon.com, Inc. or Its Affiliates
// All Rights Reserved

#pragma once

#include "s3client/s3client.hpp"
#include "sys/region_endpoints.hpp"
#include "sys/xen_aws_credentials_mgr.hpp"
#include "xen_test_utils/test_instance_metadata_setup.hpp"

#include <cppunit/config/SourcePrefix.h>
#include <cppunit/extensions/HelperMacros.h>

#include <memory>
#include <string>

extern char* gconf_redshift_region;
extern char* gconf_s3_region_endpoints;
extern char* gconf_s3_region_secondary_endpoints;

namespace aws {

class HOSMS3ClientTest {
 public:
  static std::shared_ptr<aws::S3Client> create_s3_client() {
    gconf_redshift_region = "us-east-1";
    gconf_s3_region_endpoints =
        "{\"us-east-1\": \"s3-external-1.amazonaws.com\"}";
    gconf_s3_region_secondary_endpoints =
        "{\"us-east-1\": \"s3-external-1.amazonaws.com\"}";

    instance_metadata_provider::Setup();

    CPPUNIT_ASSERT_MESSAGE(
        "gconf_redshift_region should not be null or empty.",
        gconf_redshift_region != NULL && *gconf_redshift_region != '\0');
    CPPUNIT_ASSERT_MESSAGE(
        "gconf_s3_region_endpoints should not be null or empty.",
        gconf_s3_region_endpoints != NULL &&
            *gconf_s3_region_endpoints != '\0');

    const std::string KS3Region(gconf_redshift_region);
    aws::ClientConfiguration s3_client_config(KS3Region);
    s3_client_config.setMaxConnections(10);
    s3_client_config.setHost(retrieve_region_endpoint(KS3Region,
                                                      gconf_s3_region_endpoints,
                                                      true /* throw on error */)
                                 .c_str());
    s3_client_config.setSecondaryHost(
        retrieve_region_endpoint(KS3Region, gconf_s3_region_secondary_endpoints,
                                 false)
            .c_str());

    std::shared_ptr<aws::S3Client> s3_client = std::make_shared<aws::S3Client>(
        XenAWSCredentialsMgr::buildHosmCredentialsProvider(), s3_client_config,
        traffic_type::kTest);
    CPPUNIT_ASSERT(s3_client.get());
    return s3_client;
  }
};
}  // namespace aws
