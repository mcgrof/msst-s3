#!/usr/bin/env python3
"""
Test 893: Edge test 893

Tests edge scenario 893
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_893(s3_client, config):
    """Edge test 893"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-893')
        s3_client.create_bucket(bucket_name)

        # Edge case test 893
        # Test edge case scenario 893
        key = f'edge-893.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Edge case'))
        print(f"Edge case 893: ✓")

        print(f"\nTest 893 - Edge test 893: ✓")

    except ClientError as e:
        print(f"Error in test 893: {e.response['Error']['Code']}")
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
