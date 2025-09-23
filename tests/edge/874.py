#!/usr/bin/env python3
"""
Test 874: Edge test 874

Tests edge scenario 874
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_874(s3_client, config):
    """Edge test 874"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-874')
        s3_client.create_bucket(bucket_name)

        # Edge case test 874
        # Test edge case scenario 874
        key = f'edge-874.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 874: ✓")

        print(f"\nTest 874 - Edge test 874: ✓")

    except ClientError as e:
        print(f"Error in test 874: {e.response['Error']['Code']}")
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
