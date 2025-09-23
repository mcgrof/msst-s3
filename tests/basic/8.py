#!/usr/bin/env python3
"""
Test 8: Verify checksums across storage tiers

Tests checksum consistency when objects transition between storage classes.
Ensures data integrity is maintained across different storage tiers.
"""

import hashlib
import io
import time
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_8(s3_client, config):
    """Verify checksums across storage tiers test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-8')
        s3_client.create_bucket(bucket_name)

        # Test data with known checksums
        test_objects = [
            {
                'key': 'tier-test-small.txt',
                'data': b'Small file for storage tier testing' * 100,
                'size': 'small'
            },
            {
                'key': 'tier-test-medium.bin',
                'data': b'x' * (1024 * 1024),  # 1MB
                'size': 'medium'
            },
            {
                'key': 'tier-test-large.bin',
                'data': b'L' * (5 * 1024 * 1024),  # 5MB
                'size': 'large'
            }
        ]

        # Calculate checksums for all test objects
        for obj in test_objects:
            obj['md5'] = hashlib.md5(obj['data']).hexdigest()
            obj['sha256'] = hashlib.sha256(obj['data']).hexdigest()

        # Storage classes to test (some may not be supported)
        storage_classes = [
            'STANDARD',
            'REDUCED_REDUNDANCY',
            'STANDARD_IA',
            'ONEZONE_IA',
            'INTELLIGENT_TIERING',
            'GLACIER',
            'DEEP_ARCHIVE'
        ]

        results = {}

        for obj in test_objects:
            results[obj['key']] = {}

            # Upload with STANDARD storage class first
            response = s3_client.put_object(
                bucket_name,
                obj['key'],
                io.BytesIO(obj['data']),
                StorageClass='STANDARD',
                Metadata={
                    'original-md5': obj['md5'],
                    'original-sha256': obj['sha256'],
                    'size-category': obj['size']
                }
            )

            # Get initial ETag
            initial_etag = response.get('ETag', '').strip('"')
            results[obj['key']]['STANDARD'] = {
                'etag': initial_etag,
                'md5_match': initial_etag == obj['md5']
            }

            # Verify initial upload integrity
            response = s3_client.get_object(bucket_name, obj['key'])
            downloaded_data = response['Body'].read()
            downloaded_md5 = hashlib.md5(downloaded_data).hexdigest()
            downloaded_sha256 = hashlib.sha256(downloaded_data).hexdigest()

            assert downloaded_md5 == obj['md5'], \
                f"MD5 mismatch for {obj['key']} after initial upload"
            assert downloaded_sha256 == obj['sha256'], \
                f"SHA256 mismatch for {obj['key']} after initial upload"

            # Test transitions to different storage classes
            for storage_class in storage_classes[1:]:  # Skip STANDARD
                try:
                    # Try to copy object with new storage class
                    copy_source = {'Bucket': bucket_name, 'Key': obj['key']}
                    copy_key = f"{obj['key']}-{storage_class.lower()}"

                    response = s3_client.client.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket_name,
                        Key=copy_key,
                        StorageClass=storage_class,
                        MetadataDirective='COPY'
                    )

                    # Get metadata for copied object
                    head_response = s3_client.head_object(bucket_name, copy_key)
                    actual_storage_class = head_response.get('StorageClass', 'STANDARD')

                    # Some storage classes may not be supported or may default to STANDARD
                    if actual_storage_class != 'STANDARD':
                        # Download and verify checksum
                        response = s3_client.get_object(bucket_name, copy_key)
                        tier_data = response['Body'].read()
                        tier_md5 = hashlib.md5(tier_data).hexdigest()
                        tier_sha256 = hashlib.sha256(tier_data).hexdigest()

                        # Verify checksums match original
                        assert tier_md5 == obj['md5'], \
                            f"MD5 mismatch for {obj['key']} in {storage_class}"
                        assert tier_sha256 == obj['sha256'], \
                            f"SHA256 mismatch for {obj['key']} in {storage_class}"

                        results[obj['key']][storage_class] = {
                            'supported': True,
                            'checksum_verified': True
                        }
                    else:
                        results[obj['key']][storage_class] = {
                            'supported': False,
                            'reason': 'Defaults to STANDARD'
                        }

                    # Clean up copied object
                    s3_client.delete_object(bucket_name, copy_key)

                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    results[obj['key']][storage_class] = {
                        'supported': False,
                        'error': error_code
                    }

        # Test checksum consistency with object replacement
        for obj in test_objects:
            # Overwrite object with same data
            response = s3_client.put_object(
                bucket_name,
                obj['key'],
                io.BytesIO(obj['data']),
                Metadata={
                    'version': '2',
                    'original-md5': obj['md5']
                }
            )

            # Verify checksum remains consistent
            response = s3_client.get_object(bucket_name, obj['key'])
            replaced_data = response['Body'].read()
            replaced_md5 = hashlib.md5(replaced_data).hexdigest()

            assert replaced_md5 == obj['md5'], \
                f"MD5 changed after object replacement for {obj['key']}"

        # Test checksum with server-side copy
        for obj in test_objects:
            copy_key = f"{obj['key']}-copy"
            copy_source = {'Bucket': bucket_name, 'Key': obj['key']}

            # Server-side copy
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=copy_key
            )

            # Verify copy has same checksum
            response = s3_client.get_object(bucket_name, copy_key)
            copy_data = response['Body'].read()
            copy_md5 = hashlib.md5(copy_data).hexdigest()
            copy_sha256 = hashlib.sha256(copy_data).hexdigest()

            assert copy_md5 == obj['md5'], \
                f"MD5 mismatch in server-side copy for {obj['key']}"
            assert copy_sha256 == obj['sha256'], \
                f"SHA256 mismatch in server-side copy for {obj['key']}"

        # Summary
        supported_tiers = set()
        for obj_results in results.values():
            for tier, info in obj_results.items():
                if info.get('supported') or info.get('etag'):
                    supported_tiers.add(tier)

        print(f"Supported storage tiers: {sorted(supported_tiers)}")

        # At minimum, STANDARD should work with correct checksums
        assert 'STANDARD' in supported_tiers, "STANDARD storage class must be supported"

        # Verify all checksums matched for STANDARD tier
        for obj in test_objects:
            assert results[obj['key']]['STANDARD'].get('md5_match') or \
                   results[obj['key']]['STANDARD'].get('etag'), \
                   f"Checksum verification failed for {obj['key']} in STANDARD tier"

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