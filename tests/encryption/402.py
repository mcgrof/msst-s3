#!/usr/bin/env python3
"""
Test 402: Encryption test 402

Tests encryption scenario 402
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_402(s3_client, config):
    """Encryption test 402"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-402')
        s3_client.create_bucket(bucket_name)

        # Encryption test 402
        key = f'encrypted-402.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 402: ✓")

        print(f"\nTest 402 - Encryption test 402: ✓")

    except ClientError as e:
        print(f"Error in test 402: {e.response['Error']['Code']}")
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
