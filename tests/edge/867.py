#!/usr/bin/env python3
"""
Test 867: Edge test 867

Tests edge scenario 867
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_867(s3_client, config):
    """Edge test 867"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-867')
        s3_client.create_bucket(bucket_name)

        # Edge case test 867
        # Test edge case scenario 867
        key = f'edge-867.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 867: ✓")

        print(f"\nTest 867 - Edge test 867: ✓")

    except ClientError as e:
        print(f"Error in test 867: {e.response['Error']['Code']}")
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
