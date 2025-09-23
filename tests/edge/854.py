#!/usr/bin/env python3
"""
Test 854: Edge test 854

Tests edge scenario 854
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_854(s3_client, config):
    """Edge test 854"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-854')
        s3_client.create_bucket(bucket_name)

        # Edge case test 854
        # Test edge case scenario 854
        key = f'edge-854.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 854: ✓")

        print(f"\nTest 854 - Edge test 854: ✓")

    except ClientError as e:
        print(f"Error in test 854: {e.response['Error']['Code']}")
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
