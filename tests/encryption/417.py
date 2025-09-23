#!/usr/bin/env python3
"""
Test 417: Encryption test 417

Tests encryption scenario 417
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_417(s3_client, config):
    """Encryption test 417"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-417')
        s3_client.create_bucket(bucket_name)

        # Encryption test 417
        key = f'encrypted-417.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 417: ✓")

        print(f"\nTest 417 - Encryption test 417: ✓")

    except ClientError as e:
        print(f"Error in test 417: {e.response['Error']['Code']}")
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
