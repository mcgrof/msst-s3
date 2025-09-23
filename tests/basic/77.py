#!/usr/bin/env python3
"""
Test 77: Metadata 2 entries

Tests object with 2 metadata entries
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_77(s3_client, config):
    """Metadata 2 entries"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-77')
        s3_client.create_bucket(bucket_name)

        # Test with 2 metadata entries
        metadata = {}
        for j in range(2):
            metadata[f'meta-key-{j}'] = f'meta-value-{j}'

        key = 'metadata-test.txt'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'Content'),
            Metadata=metadata
        )

        response = s3_client.head_object(bucket_name, key)
        retrieved_meta = response.get('Metadata', {})
        assert len(retrieved_meta) >= 2, "Metadata count mismatch"

        print(f"\nTest 77 - Metadata 2 entries: ✓")

    except ClientError as e:
        print(f"Error in test 77: {e.response['Error']['Code']}")
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
