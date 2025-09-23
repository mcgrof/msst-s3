#!/usr/bin/env python3
"""
Test: Metadata and Header Limits
Tests S3's handling of metadata size limits, header limits, and special metadata values
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_metadata_limits(s3_client: S3Client):
    """Test metadata and header limits"""
    bucket_name = f's3-metadata-limits-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Maximum metadata size (2KB limit for user metadata)
        print("Test 1: Maximum metadata size")
        key1 = 'metadata-size-test'
        try:
            # Create metadata just under 2KB limit
            large_metadata = {
                'x-amz-meta-large': 'A' * 1900  # Just under 2KB with header name
            }
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b'test',
                Metadata={'large': 'A' * 1900}
            )
            results['passed'].append('Metadata under 2KB limit')
            print("✓ Metadata under 2KB limit: Accepted")
        except Exception as e:
            results['failed'].append(f'Metadata under limit: {str(e)}')
            print(f"✗ Metadata under limit: {str(e)}")

        # Test 2: Over metadata size limit
        print("\nTest 2: Over metadata size limit")
        key2 = 'metadata-overlimit'
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=b'test',
                Metadata={'overlimit': 'B' * 3000}  # Well over 2KB
            )
            results['failed'].append('Over metadata limit: Should have been rejected')
            print("✗ Over metadata limit: Accepted (should reject)")
        except Exception as e:
            if 'MetadataTooLarge' in str(e) or 'RequestHeaderSectionTooLarge' in str(e):
                results['passed'].append('Over metadata limit rejected')
                print("✓ Over metadata limit: Correctly rejected")
            else:
                results['failed'].append(f'Over metadata limit: Unexpected error: {e}')

        # Test 3: Maximum number of metadata headers
        print("\nTest 3: Many metadata headers")
        key3 = 'many-headers'
        try:
            many_headers = {}
            for i in range(100):  # Try 100 different headers
                many_headers[f'header{i:03d}'] = f'value{i}'

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key3,
                Body=b'test',
                Metadata=many_headers
            )

            # Verify retrieval
            response = s3_client.client.head_object(Bucket=bucket_name, Key=key3)
            retrieved_count = sum(1 for k in response.get('Metadata', {}).keys())

            if retrieved_count == 100:
                results['passed'].append('Many metadata headers')
                print(f"✓ Many metadata headers: All {retrieved_count} preserved")
            else:
                results['failed'].append(f'Many headers: Only {retrieved_count}/100 preserved')
                print(f"✗ Many headers: Only {retrieved_count}/100 preserved")

        except Exception as e:
            results['failed'].append(f'Many headers: {str(e)}')
            print(f"✗ Many headers: {str(e)}")

        # Test 4: Special characters in metadata
        print("\nTest 4: Special characters in metadata")
        key4 = 'special-metadata'
        test_metadata = {
            'unicode': '🚀 测试 テスト',
            'spaces': 'value with spaces',
            'empty': '',
            'numbers': '12345',
            'special': '!@#$%^&*()',
        }

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'test',
                Metadata=test_metadata
            )

            # Retrieve and verify
            response = s3_client.client.head_object(Bucket=bucket_name, Key=key4)
            retrieved = response.get('Metadata', {})

            matches = 0
            for k, v in test_metadata.items():
                if retrieved.get(k) == v:
                    matches += 1

            if matches == len(test_metadata):
                results['passed'].append('Special characters in metadata')
                print(f"✓ Special characters in metadata: All preserved")
            else:
                results['failed'].append(f'Special metadata: Only {matches}/{len(test_metadata)} preserved')
                print(f"✗ Special metadata: Only {matches}/{len(test_metadata)} preserved")

        except Exception as e:
            results['failed'].append(f'Special metadata: {str(e)}')
            print(f"✗ Special metadata: {str(e)}")

        # Test 5: Reserved metadata headers
        print("\nTest 5: Reserved metadata headers")
        key5 = 'reserved-headers'
        try:
            # Try to set system headers via metadata
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key5,
                Body=b'test',
                Metadata={
                    'content-type': 'fake/type',  # Should not override actual content-type
                    'content-length': '999',      # Should not override actual length
                    'etag': 'fake-etag'           # Should not override actual etag
                }
            )

            # Verify these didn't override system values
            response = s3_client.client.head_object(Bucket=bucket_name, Key=key5)

            if response['ContentLength'] == 4:  # Actual content is 'test' = 4 bytes
                results['passed'].append('Reserved headers not overridden')
                print("✓ Reserved headers: System values preserved")
            else:
                results['failed'].append('Reserved headers: System values overridden')
                print("✗ Reserved headers: System values overridden")

        except Exception as e:
            results['failed'].append(f'Reserved headers: {str(e)}')
            print(f"✗ Reserved headers: {str(e)}")

        # Test 6: Metadata key name limits
        print("\nTest 6: Metadata key name limits")
        key6 = 'key-name-limits'
        try:
            # Very long metadata key name
            long_key = 'x' * 200  # Try a 200-char key name
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key6,
                Body=b'test',
                Metadata={long_key: 'value'}
            )
            results['passed'].append('Long metadata key name')
            print("✓ Long metadata key name: Accepted")
        except Exception as e:
            if 'InvalidArgument' in str(e):
                results['passed'].append('Long key name rejected')
                print("✓ Long metadata key name: Correctly rejected")
            else:
                results['failed'].append(f'Long key name: {str(e)}')

        # Summary
        print(f"\n=== Metadata Limits Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3_client.client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            s3_client.delete_bucket(bucket_name)
        except:
            pass

if __name__ == "__main__":
    s3 = S3Client(
        endpoint_url='http://localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        verify_ssl=False
    )
    test_metadata_limits(s3)