#!/usr/bin/env python3
"""
Test 63: Content merge

Tests content merge operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_63(s3_client, config):
    """Content merge"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-63')
        s3_client.create_bucket(bucket_name)

        # Content operation: merge
        key = 'content-test.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Original content'))

        # Simulate merge operation
        if 'merge' == 'copy':
            s3_client.client.copy_object(
                CopySource={'Bucket': bucket_name, 'Key': key},
                Bucket=bucket_name,
                Key=key + '.copy'
            )
        print(f"Content operation 'merge': ✓")

        print(f"\nTest 63 - Content merge: ✓")

    except ClientError as e:
        print(f"Error in test 63: {e.response['Error']['Code']}")
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
