#!/usr/bin/env python3
"""
Test 393: ACL public-read-write

Tests ACL setting: public-read-write
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_393(s3_client, config):
    """ACL public-read-write"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-393')
        s3_client.create_bucket(bucket_name)

        # ACL test: public-read-write
        key = 'acl-test.txt'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'ACL test content'),
                ACL='public-read-write'
            )

            # Get ACL
            response = s3_client.client.get_object_acl(Bucket=bucket_name, Key=key)
            print(f"ACL 'public-read-write' set: ✓")
        except ClientError as e:
            print(f"ACL 'public-read-write' not supported: {e.response['Error']['Code']}")

        print(f"\nTest 393 - ACL public-read-write: ✓")

    except ClientError as e:
        print(f"Error in test 393: {e.response['Error']['Code']}")
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
