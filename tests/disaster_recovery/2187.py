#!/usr/bin/env python3
"""
Test 2187: Backup validation 2187

Tests backup integrity validation
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2187(s3_client, config):
    """Backup validation 2187"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2187')
        s3_client.create_bucket(bucket_name)

        # Test backup validation and verification
        key = f'validated-backup-2187.dat'

        # Create data with checksums
        original_data = f'Important data 2187 that needs backup'.encode() * 100
        checksum = hashlib.sha256(original_data).hexdigest()

        # Upload with validation metadata
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(original_data),
            Metadata={
                'original-checksum': checksum,
                'backup-date': time.strftime('%Y-%m-%d'),
                'validation-status': 'pending',
                'backup-size': str(len(original_data))
            }
        )

        # Validate backup integrity
        response = s3_client.get_object(bucket_name, key)
        retrieved_data = response['Body'].read()
        retrieved_checksum = hashlib.sha256(retrieved_data).hexdigest()

        assert checksum == retrieved_checksum, "Backup validation failed"
        assert len(retrieved_data) == len(original_data), "Backup size mismatch"

        print(f"Backup validation test 2187: ✓")

        print(f"\nTest 2187 - Backup validation 2187: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2187 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2187: {error_code}")
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
