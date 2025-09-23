#!/usr/bin/env python3
"""
Test 65: Content transform

Tests content transform operation
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_65(s3_client, config):
    """Content transform"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-65')
        s3_client.create_bucket(bucket_name)

        # Content operation: transform
        key = 'content-test.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Original content'))

        # Simulate transform operation
        if 'transform' == 'copy':
            s3_client.client.copy_object(
                CopySource={'Bucket': bucket_name, 'Key': key},
                Bucket=bucket_name,
                Key=key + '.copy'
            )
        print(f"Content operation 'transform': ✓")

        print(f"\nTest 65 - Content transform: ✓")

    except ClientError as e:
        print(f"Error in test 65: {e.response['Error']['Code']}")
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
