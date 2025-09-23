#!/usr/bin/env python3
"""
Test 683: Performance test 683

Tests performance scenario 683
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_683(s3_client, config):
    """Performance test 683"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-683')
        s3_client.create_bucket(bucket_name)

        # Performance test 683
        import time
        start = time.time()
        for j in range(10):
            key = f'perf-683-{j}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Performance test'))
        elapsed = time.time() - start
        print(f"Performance test 683: {elapsed:.2f}s")

        print(f"\nTest 683 - Performance test 683: ✓")

    except ClientError as e:
        print(f"Error in test 683: {e.response['Error']['Code']}")
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
