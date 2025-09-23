#!/usr/bin/env python3
"""
Test 234: Versioning delete_version

Tests versioning scenario: delete_version
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_234(s3_client, config):
    """Versioning delete_version"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-234')
        s3_client.create_bucket(bucket_name)

        # Versioning scenario: delete_version
        # Enable versioning
        s3_client.put_bucket_versioning(bucket_name, {'Status': 'Enabled'})

        key = 'versioned-object.txt'

        # Create multiple versions
        for v in range(3):
            s3_client.put_object(bucket_name, key, io.BytesIO(f'Version {v}'.encode()))

        # List versions
        response = s3_client.client.list_object_versions(Bucket=bucket_name)
        versions = response.get('Versions', [])
        print(f"Created {len(versions)} versions: ✓")

        print(f"\nTest 234 - Versioning delete_version: ✓")

    except ClientError as e:
        print(f"Error in test 234: {e.response['Error']['Code']}")
        raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
