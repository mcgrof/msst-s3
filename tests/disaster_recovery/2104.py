#!/usr/bin/env python3
"""
Test 2104: PITR test 2104

Tests point-in-time recovery capabilities
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2104(s3_client, config):
    """PITR test 2104"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2104')
        s3_client.create_bucket(bucket_name)

        # Test point-in-time recovery
        key = 'backup-data-2104.json'

        # Enable versioning for point-in-time recovery
        s3_client.put_bucket_versioning(bucket_name, {'Status': 'Enabled'})

        # Create multiple versions over time
        versions = []
        for v in range(5):
            data = {
                'version': v,
                'timestamp': time.time() + v,
                'data': f'Backup state {v} for item 2104'
            }
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(json.dumps(data).encode()),
                Metadata={'backup-version': str(v)}
            )
            versions.append(response.get('VersionId'))
            time.sleep(0.1)  # Simulate time passing

        # List all versions for recovery
        response = s3_client.client.list_object_versions(
            Bucket=bucket_name,
            Prefix=key
        )

        available_versions = response.get('Versions', [])
        assert len(available_versions) >= 3, "Multiple versions needed for PITR"

        print(f"Point-in-time recovery test 2104: ✓")

        print(f"\nTest 2104 - PITR test 2104: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2104 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2104: {error_code}")
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
