#!/usr/bin/env python3
"""
Test 955: Integration test 955

Tests integration scenario 955
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_955(s3_client, config):
    """Integration test 955"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-955')
        s3_client.create_bucket(bucket_name)

        # Integration test 955
        # Test integration scenario 955
        key = f'integration-955.txt'
        s3_client.put_object(bucket_name, key, io.BytesIO(b'Integration test'))
        print(f"Integration test 955: ✓")

        print(f"\nTest 955 - Integration test 955: ✓")

    except ClientError as e:
        print(f"Error in test 955: {e.response['Error']['Code']}")
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
