#!/usr/bin/env python3
"""
Test 879: Edge test 879

Tests edge scenario 879
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_879(s3_client, config):
    """Edge test 879"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-879')
        s3_client.create_bucket(bucket_name)

        # Edge case test 879
        # Test edge case scenario 879
        key = f'edge-879.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 879: ✓")

        print(f"\nTest 879 - Edge test 879: ✓")

    except ClientError as e:
        print(f"Error in test 879: {e.response['Error']['Code']}")
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
