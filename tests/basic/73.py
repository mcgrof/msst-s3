#!/usr/bin/env python3
"""
Test 73: Batch 80 objects

Tests batch operations with 80 objects
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_73(s3_client, config):
    """Batch 80 objects"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-73')
        s3_client.create_bucket(bucket_name)

        # Batch operation with 80 objects
        for j in range(80):
            key = f'batch/object-{j:04d}.txt'
            s3_client.put_object(bucket_name, key, io.BytesIO(b'Batch content'))

        objects = s3_client.list_objects(bucket_name, prefix='batch/')
        assert len(objects) == 80, f"Expected 80 objects, found {len(objects)}"

        print(f"\nTest 73 - Batch 80 objects: ✓")

    except ClientError as e:
        print(f"Error in test 73: {e.response['Error']['Code']}")
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
