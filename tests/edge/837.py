#!/usr/bin/env python3
"""
Test 837: Edge test 837

Tests edge scenario 837
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_837(s3_client, config):
    """Edge test 837"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-837')
        s3_client.create_bucket(bucket_name)

        # Edge case test 837
        # Test edge case scenario 837
        key = f'edge-837.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 837: ✓")

        print(f"\nTest 837 - Edge test 837: ✓")

    except ClientError as e:
        print(f"Error in test 837: {e.response['Error']['Code']}")
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
