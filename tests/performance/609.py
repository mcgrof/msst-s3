#!/usr/bin/env python3
"""
Test 609: Performance test 609

Tests performance scenario 609
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_609(s3_client, config):
    """Performance test 609"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-609')
        s3_client.create_bucket(bucket_name)

        # Performance test 609
        import time
        start = time.time()
        for j in range(10):
            key = f'perf-609-{j}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Performance test'))
        elapsed = time.time() - start
        print(f"Performance test 609: {elapsed:.2f}s")

        print(f"\nTest 609 - Performance test 609: ✓")

    except ClientError as e:
        print(f"Error in test 609: {e.response['Error']['Code']}")
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
