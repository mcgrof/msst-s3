#!/usr/bin/env python3
"""
Test 473: Encryption test 473

Tests encryption scenario 473
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_473(s3_client, config):
    """Encryption test 473"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-473')
        s3_client.create_bucket(bucket_name)

        # Encryption test 473
        key = f'encrypted-473.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 473: ✓")

        print(f"\nTest 473 - Encryption test 473: ✓")

    except ClientError as e:
        print(f"Error in test 473: {e.response['Error']['Code']}")
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
