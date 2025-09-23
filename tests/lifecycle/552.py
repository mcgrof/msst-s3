#!/usr/bin/env python3
"""
Test 552: Lifecycle test 552

Tests lifecycle scenario 552
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_552(s3_client, config):
    """Lifecycle test 552"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-552')
        s3_client.create_bucket(bucket_name)

        # Lifecycle test 552
        key = f'lifecycle-552.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Lifecycle content'))
        print(f"Lifecycle test 552: ✓")

        print(f"\nTest 552 - Lifecycle test 552: ✓")

    except ClientError as e:
        print(f"Error in test 552: {e.response['Error']['Code']}")
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
