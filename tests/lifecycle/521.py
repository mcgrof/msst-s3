#!/usr/bin/env python3
"""
Test 521: Lifecycle test 521

Tests lifecycle scenario 521
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_521(s3_client, config):
    """Lifecycle test 521"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-521')
        s3_client.create_bucket(bucket_name)

        # Lifecycle test 521
        key = f'lifecycle-521.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Lifecycle content'))
        print(f"Lifecycle test 521: ✓")

        print(f"\nTest 521 - Lifecycle test 521: ✓")

    except ClientError as e:
        print(f"Error in test 521: {e.response['Error']['Code']}")
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
