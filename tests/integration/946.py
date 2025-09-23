#!/usr/bin/env python3
"""
Test 946: Integration test 946

Tests integration scenario 946
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_946(s3_client, config):
    """Integration test 946"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-946')
        s3_client.create_bucket(bucket_name)

        # Integration test 946
        # Test integration scenario 946
        key = f'integration-946.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 946: ✓")

        print(f"\nTest 946 - Integration test 946: ✓")

    except ClientError as e:
        print(f"Error in test 946: {e.response['Error']['Code']}")
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
