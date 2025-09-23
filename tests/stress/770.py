#!/usr/bin/env python3
"""
Test 770: Stress test 770

Tests stress scenario 770
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_770(s3_client, config):
    """Stress test 770"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-770')
        s3_client.create_bucket(bucket_name)

        # Stress test 770
        for j in range(50):
            key = f'stress-770-{j}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'X' * 1024))
        print(f"Stress test 770: ✓")

        print(f"\nTest 770 - Stress test 770: ✓")

    except ClientError as e:
        print(f"Error in test 770: {e.response['Error']['Code']}")
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
