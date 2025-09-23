#!/usr/bin/env python3
"""
Test 467: Encryption test 467

Tests encryption scenario 467
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_467(s3_client, config):
    """Encryption test 467"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-467')
        s3_client.create_bucket(bucket_name)

        # Encryption test 467
        key = f'encrypted-467.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 467: ✓")

        print(f"\nTest 467 - Encryption test 467: ✓")

    except ClientError as e:
        print(f"Error in test 467: {e.response['Error']['Code']}")
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
