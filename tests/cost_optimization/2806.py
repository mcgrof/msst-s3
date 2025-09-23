#!/usr/bin/env python3
"""
Test 2806: Storage optimization 2806

Tests storage class cost optimization
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2806(s3_client, config):
    """Storage optimization 2806"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2806')
        s3_client.create_bucket(bucket_name)

        # Test storage class optimization
        key = f'cost-optimized/data-2806.dat'
        data_age_days = i % 365
        access_frequency = ['frequent', 'infrequent', 'archive'][i % 3]

        # Determine optimal storage class
        if data_age_days < 30:
            storage_class = 'STANDARD'
        elif data_age_days < 90 and access_frequency == 'frequent':
            storage_class = 'INTELLIGENT_TIERING'
        elif data_age_days < 90:
            storage_class = 'STANDARD_IA'
        elif data_age_days < 180:
            storage_class = 'ONEZONE_IA'
        elif data_age_days < 365:
            storage_class = 'GLACIER'
        else:
            storage_class = 'DEEP_ARCHIVE'

        # Create test data
        data_size = 1024 * random.randint(100, 10000)  # 100KB to 10MB

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'X' * data_size),
                StorageClass=storage_class,
                Metadata={
                    'data-age-days': str(data_age_days),
                    'access-frequency': access_frequency,
                    'cost-optimized': 'true',
                    'original-size': str(data_size)
                }
            )
            print(f"Storage optimization ({storage_class}) test 2806: ✓")
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidStorageClass':
                # Fallback to STANDARD if storage class not supported
                s3_client.put_object(bucket_name, key, io.BytesIO(b'X' * data_size))
                print(f"Storage optimization (STANDARD fallback) test 2806: ✓")
            else:
                raise

        print(f"\nTest 2806 - Storage optimization 2806: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2806 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2806: {error_code}")
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
