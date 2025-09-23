#!/usr/bin/env python3
"""
Test 38: Object size 3072 bytes

Tests handling of 3072 byte object
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_38(s3_client, config):
    """Object size 3072 bytes"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-38')
        s3_client.create_bucket(bucket_name)

        # Test with 3072 byte object
        key = 'object-3072b.bin'
        data = b'X' * 3072
        s3_client.put_object(bucket_name, key, io.BytesIO(data))

        response = s3_client.get_object(bucket_name, key)
        retrieved = response['Body'].read()
        assert len(retrieved) == 3072, f"Size mismatch: expected 3072, got {len(retrieved)}"

        print(f"\nTest 38 - Object size 3072 bytes: ✓")

    except ClientError as e:
        print(f"Error in test 38: {e.response['Error']['Code']}")
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
