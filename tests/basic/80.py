#!/usr/bin/env python3
"""
Test 80: Metadata 5 entries

Tests object with 5 metadata entries
"""

import io
import time
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_80(s3_client, config):
    """Metadata 5 entries"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-80')
        s3_client.create_bucket(bucket_name)

        # Test with 5 metadata entries
        metadata = {}
        for j in range(5):
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
        assert len(retrieved_meta) >= 5, "Metadata count mismatch"

        print(f"\nTest 80 - Metadata 5 entries: ✓")

    except ClientError as e:
        print(f"Error in test 80: {e.response['Error']['Code']}")
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
