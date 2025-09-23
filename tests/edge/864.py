#!/usr/bin/env python3
"""
Test 864: Edge test 864

Tests edge scenario 864
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_864(s3_client, config):
    """Edge test 864"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-864')
        s3_client.create_bucket(bucket_name)

        # Edge case test 864
        # Test edge case scenario 864
        key = f'edge-864.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 864: ✓")

        print(f"\nTest 864 - Edge test 864: ✓")

    except ClientError as e:
        print(f"Error in test 864: {e.response['Error']['Code']}")
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
