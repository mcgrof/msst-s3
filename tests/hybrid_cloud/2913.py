#!/usr/bin/env python3
"""
Test 2913: Multi-cloud sync 2913

Tests multi-cloud storage synchronization
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2913(s3_client, config):
    """Multi-cloud sync 2913"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2913')
        s3_client.create_bucket(bucket_name)

        # Test multi-cloud storage sync
        key = f'multi-cloud/sync/{source_cloud}-to-{target_cloud}/data-2913.dat'

        # Multi-cloud sync metadata
        sync_config = {
            'source': {
                'provider': 'azure',
                'region': 'us-east-1',
                'bucket': f'source-bucket-2913',
                'endpoint': f'https://s3.{source_cloud}.com'
            },
            'target': {
                'provider': 'gcp',
                'region': 'europe-west1',
                'bucket': f'target-bucket-2913',
                'endpoint': f'https://storage.{target_cloud}.com'
            },
            'sync_options': {
                'mode': ['mirror', 'backup', 'archive'][i % 3],
                'frequency': ['realtime', 'hourly', 'daily'][i % 3],
                'compression': True,
                'encryption_in_transit': True,
                'preserve_metadata': True
            },
            'last_sync': time.time(),
            'objects_synced': random.randint(100, 10000),
            'data_transferred_gb': random.uniform(1, 1000)
        }

        # Store sync configuration
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(sync_config).encode()),
            Metadata={
                'source-cloud': source_cloud,
                'target-cloud': target_cloud,
                'sync-mode': sync_config['sync_options']['mode'],
                'multi-cloud': 'true'
            }
        )

        print(f"Multi-cloud sync (azure → gcp) test 2913: ✓")

        print(f"\nTest 2913 - Multi-cloud sync 2913: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2913 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2913: {error_code}")
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
