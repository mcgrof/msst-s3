#!/usr/bin/env python3
"""
Test 912: Integration test 912

Tests integration scenario 912
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_912(s3_client, config):
    """Integration test 912"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-912')
        s3_client.create_bucket(bucket_name)

        # Integration test 912
        # Test integration scenario 912
        key = f'integration-912.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 912: ✓")

        print(f"\nTest 912 - Integration test 912: ✓")

    except ClientError as e:
        print(f"Error in test 912: {e.response['Error']['Code']}")
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
