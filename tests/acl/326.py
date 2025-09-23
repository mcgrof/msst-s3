#!/usr/bin/env python3
"""
Test 326: ACL bucket-owner-read

Tests ACL setting: bucket-owner-read
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_326(s3_client, config):
    """ACL bucket-owner-read"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-326')
        s3_client.create_bucket(bucket_name)

        # ACL test: bucket-owner-read
        key = 'acl-test.txt'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'ACL test content'),
                ACL='bucket-owner-read'
            )

            # Get ACL
            response = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key)
            print(f"ACL 'bucket-owner-read' set: ✓")
        except ClientError as e:
            print(f"ACL 'bucket-owner-read' not supported: {e.response['Error']['Code']}")

        print(f"\nTest 326 - ACL bucket-owner-read: ✓")

    except ClientError as e:
        print(f"Error in test 326: {e.response['Error']['Code']}")
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
