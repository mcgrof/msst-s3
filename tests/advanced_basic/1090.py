#!/usr/bin/env python3
"""
Test 1090: Complex metadata 1090

Tests complex metadata scenario 1090
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1090(s3_client, config):
    """Complex metadata 1090"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1090')
        s3_client.create_bucket(bucket_name)

        # Test complex metadata scenarios
        key = 'complex-metadata.txt'
        metadata = {
            'user-id': str(1090),
            'timestamp': str(time.time()),
            'hash': hashlib.md5(str(1090).encode()).hexdigest(),
            'json-data': json.dumps({'test_id': 1090, 'nested': {'value': 'test'}}),
            'unicode': '测试数据-1090',
            'special-chars': '!@#$%^&*()_+-=[]{}|;:,.<>?',
            'long-value': 'x' * 500  # Long metadata value
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(f'Test {i}'.encode()),
            Metadata=metadata
        )

        # Verify metadata preservation
        response = s3_client.head_object(bucket_name, key)
        retrieved = response.get('Metadata', {})
        assert 'user-id' in retrieved, "Metadata not preserved"

        print(f"\nTest 1090 - Complex metadata 1090: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1090 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1090: {error_code}")
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
