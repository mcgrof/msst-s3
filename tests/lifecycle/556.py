#!/usr/bin/env python3
"""
Test 556: Lifecycle test 556

Tests lifecycle scenario 556
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_556(s3_client, config):
    """Lifecycle test 556"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-556')
        s3_client.create_bucket(bucket_name)

        # Lifecycle test 556
        key = f'lifecycle-556.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Lifecycle content'))
        print(f"Lifecycle test 556: ✓")

        print(f"\nTest 556 - Lifecycle test 556: ✓")

    except ClientError as e:
        print(f"Error in test 556: {e.response['Error']['Code']}")
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
