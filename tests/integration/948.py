#!/usr/bin/env python3
"""
Test 948: Integration test 948

Tests integration scenario 948
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_948(s3_client, config):
    """Integration test 948"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-948')
        s3_client.create_bucket(bucket_name)

        # Integration test 948
        # Test integration scenario 948
        key = f'integration-948.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 948: ✓")

        print(f"\nTest 948 - Integration test 948: ✓")

    except ClientError as e:
        print(f"Error in test 948: {e.response['Error']['Code']}")
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
