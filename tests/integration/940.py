#!/usr/bin/env python3
"""
Test 940: Integration test 940

Tests integration scenario 940
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_940(s3_client, config):
    """Integration test 940"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-940')
        s3_client.create_bucket(bucket_name)

        # Integration test 940
        # Test integration scenario 940
        key = f'integration-940.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 940: ✓")

        print(f"\nTest 940 - Integration test 940: ✓")

    except ClientError as e:
        print(f"Error in test 940: {e.response['Error']['Code']}")
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
