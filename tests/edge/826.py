#!/usr/bin/env python3
"""
Test 826: Edge test 826

Tests edge scenario 826
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_826(s3_client, config):
    """Edge test 826"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-826')
        s3_client.create_bucket(bucket_name)

        # Edge case test 826
        # Test edge case scenario 826
        key = f'edge-826.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 826: ✓")

        print(f"\nTest 826 - Edge test 826: ✓")

    except ClientError as e:
        print(f"Error in test 826: {e.response['Error']['Code']}")
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
