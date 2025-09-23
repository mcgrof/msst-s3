#!/usr/bin/env python3
"""
Test 820: Edge test 820

Tests edge scenario 820
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_820(s3_client, config):
    """Edge test 820"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-820')
        s3_client.create_bucket(bucket_name)

        # Edge case test 820
        # Test edge case scenario 820
        key = f'edge-820.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 820: ✓")

        print(f"\nTest 820 - Edge test 820: ✓")

    except ClientError as e:
        print(f"Error in test 820: {e.response['Error']['Code']}")
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
