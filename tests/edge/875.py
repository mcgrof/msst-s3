#!/usr/bin/env python3
"""
Test 875: Edge test 875

Tests edge scenario 875
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_875(s3_client, config):
    """Edge test 875"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-875')
        s3_client.create_bucket(bucket_name)

        # Edge case test 875
        # Test edge case scenario 875
        key = f'edge-875.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 875: ✓")

        print(f"\nTest 875 - Edge test 875: ✓")

    except ClientError as e:
        print(f"Error in test 875: {e.response['Error']['Code']}")
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
