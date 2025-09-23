#!/usr/bin/env python3
"""
Test 2146: Incremental backup 2146

Tests incremental backup strategy
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2146(s3_client, config):
    """Incremental backup 2146"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2146')
        s3_client.create_bucket(bucket_name)

        # Test incremental backup strategy
        base_key = f'incremental/base-2146.dat'

        # Create base backup
        base_data = b'BASE' * 1024 * 100  # 400KB base
        s3_client.put_object(bucket_name, base_key, io.BytesIO(base_data))

        # Create incremental backups
        for inc in range(3):
            inc_key = f'incremental/delta-2146-{inc}.dat'
            delta_data = f'DELTA-{inc}'.encode() * 1024 * 10  # 40KB increments

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=inc_key,
                Body=io.BytesIO(delta_data),
                Metadata={
                    'backup-type': 'incremental',
                    'base-backup': base_key,
                    'sequence': str(inc),
                    'timestamp': str(time.time())
                }
            )

        # Verify backup chain
        objects = s3_client.list_objects(bucket_name, prefix=f'incremental/')
        backup_chain = [o for o in objects if f'-2146' in o['Key']]
        assert len(backup_chain) >= 4, "Complete backup chain required"

        print(f"Incremental backup test 2146: ✓")

        print(f"\nTest 2146 - Incremental backup 2146: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2146 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2146: {error_code}")
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
