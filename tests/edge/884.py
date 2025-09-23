#!/usr/bin/env python3
"""
Test 884: Edge test 884

Tests edge scenario 884
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_884(s3_client, config):
    """Edge test 884"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-884')
        s3_client.create_bucket(bucket_name)

        # Edge case test 884
        # Test edge case scenario 884
        key = f'edge-884.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 884: ✓")

        print(f"\nTest 884 - Edge test 884: ✓")

    except ClientError as e:
        print(f"Error in test 884: {e.response['Error']['Code']}")
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
