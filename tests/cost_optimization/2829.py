#!/usr/bin/env python3
"""
Test 2829: Compression 2829

Tests data compression for cost savings
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2829(s3_client, config):
    """Compression 2829"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2829')
        s3_client.create_bucket(bucket_name)

        # Test compression strategies
        key = f'compressed/data-2829.gz'

        # Generate compressible data
        original_data = (f'Repeated pattern 2829 ' * 1000).encode()
        original_size = len(original_data)

        # Apply compression
        import gzip
        compressed_data = gzip.compress(original_data, compresslevel=9)
        compressed_size = len(compressed_data)
        compression_ratio = compressed_size / original_size

        # Store compressed data
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(compressed_data),
            ContentEncoding='gzip',
            Metadata={
                'original-size': str(original_size),
                'compressed-size': str(compressed_size),
                'compression-ratio': f'{compression_ratio:.2f}',
                'compression-savings': f'{(1 - compression_ratio) * 100:.1f}%'
            }
        )

        print(f"Compression test 2829: {(1 - compression_ratio) * 100:.1f}% savings")

        print(f"\nTest 2829 - Compression 2829: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2829 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2829: {error_code}")
            raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Clean up all objects including versions
                try:
                    versions = s3_client.client.list_object_versions(Bucket=bucket_name)
                    for version in versions.get('Versions', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=version['Key'],
                            VersionId=version['VersionId']
                        )
                    for marker in versions.get('DeleteMarkers', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=marker['Key'],
                            VersionId=marker['VersionId']
                        )
                except:
                    # Fallback to simple deletion
                    objects = s3_client.list_objects(bucket_name)
                    for obj in objects:
                        s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass
