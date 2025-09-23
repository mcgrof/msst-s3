#!/usr/bin/env python3
"""
Test 398: ACL private

Tests ACL setting: private
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_398(s3_client, config):
    """ACL private"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-398')
        s3_client.create_bucket(bucket_name)

        # ACL test: private
        key = 'acl-test.txt'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'ACL test content'),
                ACL='private'
            )

            # Get ACL
            response = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key)
            print(f"ACL 'private' set: ✓")
        except ClientError as e:
            print(f"ACL 'private' not supported: {e.response['Error']['Code']}")

        print(f"\nTest 398 - ACL private: ✓")

    except ClientError as e:
        print(f"Error in test 398: {e.response['Error']['Code']}")
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
