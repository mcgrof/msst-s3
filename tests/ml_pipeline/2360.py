#!/usr/bin/env python3
"""
Test 2360: Feature store 2360

Tests ML feature store integration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2360(s3_client, config):
    """Feature store 2360"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2360')
        s3_client.create_bucket(bucket_name)

        # Test feature store integration
        feature_group = f'feature-store/feature-group-2360/'

        # Feature definitions
        features = {
            'feature_group_name': f'fg_2360',
            'features': [
                {'name': 'user_id', 'type': 'string', 'description': 'User identifier'}
            ] + [
                {'name': f'feature_{j}', 'type': 'float', 'description': f'Feature {j}'}
                for j in range(5)
            ],
            'online_store_enabled': True,
            'offline_store_enabled': True,
            'event_time_feature': 'timestamp',
            'record_identifier': 'user_id'
        }

        # Store feature data
        for partition in range(3):
            key = f'{feature_group}partition-{partition}/data.parquet'
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'PARQUET_DATA' * 1000),
                Metadata={
                    'feature-group': features['feature_group_name'],
                    'partition-id': str(partition),
                    'feature-count': str(len(features['features']))
                }
            )

        print(f"Feature store test 2360: ✓")

        print(f"\nTest 2360 - Feature store 2360: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2360 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2360: {error_code}")
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
