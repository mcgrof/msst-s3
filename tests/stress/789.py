#!/usr/bin/env python3
"""
Test 789: Stress test 789

Tests stress scenario 789
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_789(s3_client, config):
    """Stress test 789"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-789')
        s3_client.create_bucket(bucket_name)

        # Stress test 789
        for j in range(50):
            key = f'stress-789-{j}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'X' * 1024))
        print(f"Stress test 789: ✓")

        print(f"\nTest 789 - Stress test 789: ✓")

    except ClientError as e:
        print(f"Error in test 789: {e.response['Error']['Code']}")
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
