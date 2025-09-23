#!/usr/bin/env python3
"""
Test 001: Create and delete an empty bucket

Tests basic bucket creation and deletion operations.
Verifies that a bucket can be created and then deleted successfully.
"""

from common.fixtures import TestFixture
from common.validators import validate_bucket_exists, validate_bucket_not_exists

def test_001(s3_client, config):
    """Create and delete an empty bucket"""
    fixture = TestFixture(s3_client, config)

    try:
        # Generate a unique bucket name
        bucket_name = fixture.generate_bucket_name('test-001')

        # Create the bucket
        s3_client.create_bucket(bucket_name)

        # Verify bucket exists
        validate_bucket_exists(s3_client, bucket_name)

        # List buckets and verify our bucket is present
        buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in buckets]
        assert bucket_name in bucket_names, f"Bucket {bucket_name} not found in bucket list"

        # Delete the bucket
        s3_client.delete_bucket(bucket_name)

        # Verify bucket no longer exists
        validate_bucket_not_exists(s3_client, bucket_name)

        # List buckets and verify our bucket is gone
        buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in buckets]
        assert bucket_name not in bucket_names, f"Bucket {bucket_name} still in bucket list after deletion"

    finally:
        # Cleanup in case of failure
        try:
            if s3_client.bucket_exists(bucket_name):
                s3_client.delete_bucket(bucket_name)
        except:
            pass
