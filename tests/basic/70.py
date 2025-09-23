#!/usr/bin/env python3
"""
Test 70: Batch 50 objects

Tests batch operations with 50 objects
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_70(s3_client, config):
    """Batch 50 objects"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-70')
        s3_client.create_bucket(bucket_name)

        # Batch operation with 50 objects
        for j in range(50):
            key = f'batch/object-{j:04d}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Batch content'))

        objects = s3_client.list_objects(bucket_name, prefix='batch/')
        assert len(objects) == 50, f"Expected 50 objects, found {len(objects)}"

        print(f"\nTest 70 - Batch 50 objects: ✓")

    except ClientError as e:
        print(f"Error in test 70: {e.response['Error']['Code']}")
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
