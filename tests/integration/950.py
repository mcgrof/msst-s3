#!/usr/bin/env python3
"""
Test 950: Integration test 950

Tests integration scenario 950
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_950(s3_client, config):
    """Integration test 950"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-950')
        s3_client.create_bucket(bucket_name)

        # Integration test 950
        # Test integration scenario 950
        key = f'integration-950.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 950: ✓")

        print(f"\nTest 950 - Integration test 950: ✓")

    except ClientError as e:
        print(f"Error in test 950: {e.response['Error']['Code']}")
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
