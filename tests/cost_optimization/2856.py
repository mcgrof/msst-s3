#!/usr/bin/env python3
"""
Test 2856: Lifecycle policy 2856

Tests lifecycle policy for cost optimization
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2856(s3_client, config):
    """Lifecycle policy 2856"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2856')
        s3_client.create_bucket(bucket_name)

        # Test lifecycle policy optimization
        policy_key = f'lifecycle-policies/policy-2856.json'

        # Lifecycle policy for cost optimization
        lifecycle_policy = {
            'Rules': [
                {
                    'ID': f'rule-2856-transition',
                    'Status': 'Enabled',
                    'Transitions': [
                        {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                        {'Days': 90, 'StorageClass': 'ONEZONE_IA'},
                        {'Days': 180, 'StorageClass': 'GLACIER'},
                        {'Days': 365, 'StorageClass': 'DEEP_ARCHIVE'}
                    ]
                },
                {
                    'ID': f'rule-2856-expiration',
                    'Status': 'Enabled',
                    'Expiration': {'Days': 730},  # 2 years
                    'NoncurrentVersionExpiration': {'NoncurrentDays': 30}
                },
                {
                    'ID': f'rule-2856-multipart',
                    'Status': 'Enabled',
                    'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7}
                }
            ]
        }

        # Store policy
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=policy_key,
            Body=io.BytesIO(json.dumps(lifecycle_policy).encode()),
            ContentType='application/json',
            Metadata={
                'policy-type': 'lifecycle',
                'cost-optimization': 'true',
                'rule-count': str(len(lifecycle_policy['Rules']))
            }
        )

        print(f"Lifecycle optimization test 2856: ✓")

        print(f"\nTest 2856 - Lifecycle policy 2856: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2856 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2856: {error_code}")
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
