# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from raff.datasharing.datasharing_test import (cluster_cache, consumer_cluster)

"""
Trick to get around claim of unused import even though it is used in
the usefixtures marker in the test classes.
"""
__all__ = [cluster_cache, consumer_cluster]
