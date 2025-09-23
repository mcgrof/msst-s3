#!/usr/bin/env python3
"""
Test 607: Performance test 607

Tests performance scenario 607
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_607(s3_client, config):
    """Performance test 607"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-607')
        s3_client.create_bucket(bucket_name)

        # Performance test 607
        import time
        start = time.time()
        for j in range(10):
            key = f'perf-607-{j}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Performance test'))
        elapsed = time.time() - start
        print(f"Performance test 607: {elapsed:.2f}s")

        print(f"\nTest 607 - Performance test 607: ✓")

    except ClientError as e:
        print(f"Error in test 607: {e.response['Error']['Code']}")
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
