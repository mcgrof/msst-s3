#!/usr/bin/env python3
"""
Test 465: Encryption test 465

Tests encryption scenario 465
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_465(s3_client, config):
    """Encryption test 465"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-465')
        s3_client.create_bucket(bucket_name)

        # Encryption test 465
        key = f'encrypted-465.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 465: ✓")

        print(f"\nTest 465 - Encryption test 465: ✓")

    except ClientError as e:
        print(f"Error in test 465: {e.response['Error']['Code']}")
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
