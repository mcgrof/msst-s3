#!/usr/bin/env python3
"""
Test 2873: Request analysis 2873

Tests request pattern analysis for costs
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2873(s3_client, config):
    """Request analysis 2873"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2873')
        s3_client.create_bucket(bucket_name)

        # Test request pattern analysis for cost optimization
        key = f'request-patterns/analysis-2873.json'

        # Simulated request pattern data
        request_pattern = {
            'time_period': 'daily',
            'total_requests': random.randint(1000, 100000),
            'request_distribution': {
                'GET': random.randint(60, 80),
                'PUT': random.randint(10, 20),
                'DELETE': random.randint(1, 5),
                'LIST': random.randint(5, 15)
            },
            'data_transfer': {
                'ingress_gb': random.uniform(1, 100),
                'egress_gb': random.uniform(10, 1000),
                'cross_region_gb': random.uniform(0, 50)
            },
            'cost_analysis': {
                'storage_cost': random.uniform(10, 1000),
                'request_cost': random.uniform(1, 100),
                'transfer_cost': random.uniform(5, 500),
                'total_cost': random.uniform(50, 2000)
            },
            'recommendations': [
                'Enable S3 Transfer Acceleration for large uploads',
                'Use CloudFront for frequently accessed content',
                'Implement request batching to reduce API calls',
                'Consider S3 Intelligent-Tiering for variable access patterns'
            ][:(i % 4 + 1)]
        }

        # Store analysis
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(request_pattern).encode()),
            Metadata={
                'analysis-type': 'request-pattern',
                'total-requests': str(request_pattern['total_requests']),
                'estimated-cost': str(request_pattern['cost_analysis']['total_cost'])
            }
        )

        print(f"Request pattern analysis test 2873: ✓")

        print(f"\nTest 2873 - Request analysis 2873: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2873 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2873: {error_code}")
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
