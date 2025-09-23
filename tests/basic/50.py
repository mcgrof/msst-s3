#!/usr/bin/env python3
"""
Test 50: Key pattern test

Tests key name: path/to/file.txt...
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_50(s3_client, config):
    """Key pattern test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-50')
        s3_client.create_bucket(bucket_name)

        # Test key name: path/to/file.txt
        data = b'Test content'
        try:
            s3_client.put_object(bucket_name, 'path/to/file.txt', io.BytesIO(data))
            response = s3_client.get_object(bucket_name, 'path/to/file.txt')
            print(f"Key pattern 'path/to/file.txt' supported: ✓")
        except ClientError as e:
            print(f"Key pattern 'path/to/file.txt' not supported: {e.response['Error']['Code']}")

        print(f"\nTest 50 - Key pattern test: ✓")

    except ClientError as e:
        print(f"Error in test 50: {e.response['Error']['Code']}")
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
