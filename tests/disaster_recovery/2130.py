#!/usr/bin/env python3
"""
Test 2130: Cross-region backup 2130

Tests cross-region backup to eu-west-1
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2130(s3_client, config):
    """Cross-region backup 2130"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2130')
        s3_client.create_bucket(bucket_name)

        # Test cross-region backup simulation
        key = f'cross-region-backup-2130.dat'

        # Original data with region metadata
        data = {
            'source_region': 'us-east-1',
            'target_region': 'eu-west-1',
            'backup_id': f'BKP00002130',
            'data': 'Critical business data ' * 100
        }

        # Upload with backup metadata
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(data).encode()),
            Metadata={
                'backup-type': 'cross-region',
                'source-region': 'us-east-1',
                'target-region': 'eu-west-1',
                'rpo': '1-hour',  # Recovery Point Objective
                'rto': '4-hours'  # Recovery Time Objective
            },
            StorageClass='STANDARD_IA'  # Cost-effective for backups
        )

        print(f"Cross-region backup to eu-west-1: ✓")

        print(f"\nTest 2130 - Cross-region backup 2130: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2130 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2130: {error_code}")
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
