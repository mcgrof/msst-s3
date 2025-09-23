#!/usr/bin/env python3
"""
Test 31: Object metadata operations

Tests setting and retrieving custom object metadata
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_31(s3_client, config):
    """Object metadata operations"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-31')
        s3_client.create_bucket(bucket_name)

        # Test operations
        # Set custom metadata
        key = 'metadata-test.txt'
        metadata = {
            'custom-key1': 'value1',
            'custom-key2': 'value2',
            'test-id': '031'
        }
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'Test content'),
            Metadata=metadata
        )

        # Retrieve and verify metadata
        response = s3_client.head_object(bucket_name, key)
        retrieved_metadata = response.get('Metadata', {})
        assert 'custom-key1' in retrieved_metadata, "Metadata not found"
        print("Custom metadata operations: ✓")

        print(f"\nObject metadata operations completed: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Error: {error_code}")
        raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
