#!/usr/bin/env python3
"""
Test 817: Edge test 817

Tests edge scenario 817
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_817(s3_client, config):
    """Edge test 817"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-817')
        s3_client.create_bucket(bucket_name)

        # Edge case test 817
        # Test edge case scenario 817
        key = f'edge-817.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 817: ✓")

        print(f"\nTest 817 - Edge test 817: ✓")

    except ClientError as e:
        print(f"Error in test 817: {e.response['Error']['Code']}")
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
