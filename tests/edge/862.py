#!/usr/bin/env python3
"""
Test 862: Edge test 862

Tests edge scenario 862
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_862(s3_client, config):
    """Edge test 862"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-862')
        s3_client.create_bucket(bucket_name)

        # Edge case test 862
        # Test edge case scenario 862
        key = f'edge-862.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 862: ✓")

        print(f"\nTest 862 - Edge test 862: ✓")

    except ClientError as e:
        print(f"Error in test 862: {e.response['Error']['Code']}")
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
