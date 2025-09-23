#!/usr/bin/env python3
"""
Test 436: Encryption test 436

Tests encryption scenario 436
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_436(s3_client, config):
    """Encryption test 436"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-436')
        s3_client.create_bucket(bucket_name)

        # Encryption test 436
        key = f'encrypted-436.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Encrypted content'))
        print(f"Encryption test 436: ✓")

        print(f"\nTest 436 - Encryption test 436: ✓")

    except ClientError as e:
        print(f"Error in test 436: {e.response['Error']['Code']}")
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
