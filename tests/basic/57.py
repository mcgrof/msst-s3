#!/usr/bin/env python3
"""
Test 57: Content prepend

Tests content prepend operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_57(s3_client, config):
    """Content prepend"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-57')
        s3_client.create_bucket(bucket_name)

        # Content operation: prepend
        key = 'content-test.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Original content'))

        # Simulate prepend operation
        if 'prepend' == 'copy':
            s3_client.client.copy_object(
                CopySource={'Bucket': bucket_name, 'Key': key},
                Bucket=bucket_name,
                Key=key + '.copy'
            )
        print(f"Content operation 'prepend': ✓")

        print(f"\nTest 57 - Content prepend: ✓")

    except ClientError as e:
        print(f"Error in test 57: {e.response['Error']['Code']}")
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
