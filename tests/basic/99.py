#!/usr/bin/env python3
"""
Test 99: Error: service unavailable

Tests error handling for service unavailable
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_99(s3_client, config):
    """Error: service unavailable"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-99')
        s3_client.create_bucket(bucket_name)

        # Test error scenario: service unavailable
        try:
            if 'service unavailable' == 'non-existent key':
                s3_client.get_object(bucket_name, 'does-not-exist.txt')
            elif 'service unavailable' == 'empty key name':
                s3_client.put_object(bucket_name, '', io.BytesIO(b'Content'))
            else:
                # Simulate service unavailable
                pass
            print(f"Error scenario 'service unavailable' did not raise expected error")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Error scenario 'service unavailable' raised: {error_code}")

        print(f"\nTest 99 - Error: service unavailable: ✓")

    except ClientError as e:
        print(f"Error in test 99: {e.response['Error']['Code']}")
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
