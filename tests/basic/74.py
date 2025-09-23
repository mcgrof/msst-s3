#!/usr/bin/env python3
"""
Test 74: Batch 90 objects

Tests batch operations with 90 objects
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_74(s3_client, config):
    """Batch 90 objects"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-74')
        s3_client.create_bucket(bucket_name)

        # Batch operation with 90 objects
        for j in range(90):
            key = f'batch/object-{j:04d}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Batch content'))

        objects = s3_client.list_objects(bucket_name, prefix='batch/')
        assert len(objects) == 90, f"Expected 90 objects, found {len(objects)}"

        print(f"\nTest 74 - Batch 90 objects: ✓")

    except ClientError as e:
        print(f"Error in test 74: {e.response['Error']['Code']}")
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
