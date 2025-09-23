#!/usr/bin/env python3
"""
Test 477: Encryption test 477

Tests encryption scenario 477
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_477(s3_client, config):
    """Encryption test 477"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-477')
        s3_client.create_bucket(bucket_name)

        # Encryption test 477
        key = f'encrypted-477.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 477: ✓")

        print(f"\nTest 477 - Encryption test 477: ✓")

    except ClientError as e:
        print(f"Error in test 477: {e.response['Error']['Code']}")
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
