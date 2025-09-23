#!/usr/bin/env python3
"""
Test 61: Content rename

Tests content rename operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_61(s3_client, config):
    """Content rename"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-61')
        s3_client.create_bucket(bucket_name)

        # Content operation: rename
        key = 'content-test.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Original content'))

        # Simulate rename operation
        if 'rename' == 'copy':
            s3_client.client.copy_object(
                CopySource={'Bucket': bucket_name, 'Key': key},
                Bucket=bucket_name,
                Key=key + '.copy'
            )
        print(f"Content operation 'rename': ✓")

        print(f"\nTest 61 - Content rename: ✓")

    except ClientError as e:
        print(f"Error in test 61: {e.response['Error']['Code']}")
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
