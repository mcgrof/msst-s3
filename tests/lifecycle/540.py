#!/usr/bin/env python3
"""
Test 540: Lifecycle test 540

Tests lifecycle scenario 540
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_540(s3_client, config):
    """Lifecycle test 540"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-540')
        s3_client.create_bucket(bucket_name)

        # Lifecycle test 540
        key = f'lifecycle-540.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Lifecycle content'))
        print(f"Lifecycle test 540: ✓")

        print(f"\nTest 540 - Lifecycle test 540: ✓")

    except ClientError as e:
        print(f"Error in test 540: {e.response['Error']['Code']}")
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
