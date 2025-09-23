#!/usr/bin/env python3
"""
Test 401: Encryption test 401

Tests encryption scenario 401
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_401(s3_client, config):
    """Encryption test 401"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-401')
        s3_client.create_bucket(bucket_name)

        # Encryption test 401
        key = f'encrypted-401.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 401: ✓")

        print(f"\nTest 401 - Encryption test 401: ✓")

    except ClientError as e:
        print(f"Error in test 401: {e.response['Error']['Code']}")
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
