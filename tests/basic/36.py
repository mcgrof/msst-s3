#!/usr/bin/env python3
"""
Test 36: Object size 1024 bytes

Tests handling of 1024 byte object
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_36(s3_client, config):
    """Object size 1024 bytes"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-36')
        s3_client.create_bucket(bucket_name)

        # Test with 1024 byte object
        key = 'object-1024b.bin'
        data = b'X' * 1024
        s3_client.put_object(bucket_name, key, io.BytesIO(data))

        response = s3_client.get_object(bucket_name, key)
        retrieved = response['Body'].read()
        assert len(retrieved) == 1024, f"Size mismatch: expected 1024, got {len(retrieved)}"

        print(f"\nTest 36 - Object size 1024 bytes: ✓")

    except ClientError as e:
        print(f"Error in test 36: {e.response['Error']['Code']}")
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
