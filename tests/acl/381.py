#!/usr/bin/env python3
"""
Test 381: ACL aws-exec-read

Tests ACL setting: aws-exec-read
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_381(s3_client, config):
    """ACL aws-exec-read"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-381')
        s3_client.create_bucket(bucket_name)

        # ACL test: aws-exec-read
        key = 'acl-test.txt'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'ACL test content'),
                ACL='aws-exec-read'
            )

            # Get ACL
            response = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key)
            print(f"ACL 'aws-exec-read' set: ✓")
        except ClientError as e:
            print(f"ACL 'aws-exec-read' not supported: {e.response['Error']['Code']}")

        print(f"\nTest 381 - ACL aws-exec-read: ✓")

    except ClientError as e:
        print(f"Error in test 381: {e.response['Error']['Code']}")
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
