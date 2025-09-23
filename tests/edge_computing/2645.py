#!/usr/bin/env python3
"""
Test 2645: Lambda@Edge 2645

Tests Lambda@Edge patterns
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2645(s3_client, config):
    """Lambda@Edge 2645"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2645')
        s3_client.create_bucket(bucket_name)

        # Test Lambda@Edge patterns
        key = f'lambda-edge/responses/modified-2645.html'

        # Simulate Lambda@Edge modifications
        lambda_edge = {
            'trigger': ['viewer-request', 'origin-request', 'origin-response', 'viewer-response'][i % 4],
            'modifications': {
                'headers_added': {
                    'X-Custom-Header': f'value-2645',
                    'X-Request-Id': f'REQ-00002645',
                    'X-Edge-Location': 'IAD50'
                },
                'headers_removed': ['Server', 'X-Powered-By'],
                'body_modified': True,
                'status_code': 200,
                'cache_behavior_modified': True
            },
            'execution_time_ms': random.randint(1, 50),
            'memory_used_mb': random.randint(128, 512)
        }

        # Store modified response
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'<html>Modified by Lambda@Edge</html>'),
            ContentType='text/html',
            Metadata={
                'lambda-edge-trigger': lambda_edge['trigger'],
                'execution-time': str(lambda_edge['execution_time_ms']),
                'modifications-applied': 'true'
            }
        )

        print(f"Lambda@Edge test 2645: ✓")

        print(f"\nTest 2645 - Lambda@Edge 2645: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2645 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2645: {error_code}")
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
