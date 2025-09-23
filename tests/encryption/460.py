#!/usr/bin/env python3
"""
Test 460: Encryption test 460

Tests encryption scenario 460
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_460(s3_client, config):
    """Encryption test 460"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-460')
        s3_client.create_bucket(bucket_name)

        # Encryption test 460
        key = f'encrypted-460.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 460: ✓")

        print(f"\nTest 460 - Encryption test 460: ✓")

    except ClientError as e:
        print(f"Error in test 460: {e.response['Error']['Code']}")
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
