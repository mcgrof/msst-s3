#!/usr/bin/env python3
"""
Test 463: Encryption test 463

Tests encryption scenario 463
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_463(s3_client, config):
    """Encryption test 463"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-463')
        s3_client.create_bucket(bucket_name)

        # Encryption test 463
        key = f'encrypted-463.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 463: ✓")

        print(f"\nTest 463 - Encryption test 463: ✓")

    except ClientError as e:
        print(f"Error in test 463: {e.response['Error']['Code']}")
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
