#!/usr/bin/env python3
"""
Test 913: Integration test 913

Tests integration scenario 913
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_913(s3_client, config):
    """Integration test 913"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-913')
        s3_client.create_bucket(bucket_name)

        # Integration test 913
        # Test integration scenario 913
        key = f'integration-913.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 913: ✓")

        print(f"\nTest 913 - Integration test 913: ✓")

    except ClientError as e:
        print(f"Error in test 913: {e.response['Error']['Code']}")
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
