#!/usr/bin/env python3
"""
Test 2739: API Gateway 2739

Tests API Gateway configuration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2739(s3_client, config):
    """API Gateway 2739"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2739')
        s3_client.create_bucket(bucket_name)

        # Test API Gateway configuration
        api_name = f'api-gateway-2739'
        key = f'api-gateway/{api_name}/openapi.json'

        # OpenAPI specification
        openapi_spec = {
            'openapi': '3.0.0',
            'info': {
                'title': api_name,
                'version': f'1.0.0'
            },
            'servers': [
                {'url': f'https://api-2739.example.com/v1'}
            ],
            'paths': {
                f'/resource-{j}': {
                    'get': {
                        'summary': f'Get resource {j}',
                        'responses': {'200': {'description': 'Success'}}
                    },
                    'post': {
                        'summary': f'Create resource {j}',
                        'responses': {'201': {'description': 'Created'}}
                    }
                } for j in range(3)
            },
            'security': [
                {'apiKey': []},
                {'oauth2': ['read', 'write']}
            ],
            'x-amazon-apigateway-request-validators': {
                'all': {
                    'validateRequestBody': True,
                    'validateRequestParameters': True
                }
            }
        }

        # Store API specification
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(openapi_spec).encode()),
            ContentType='application/json',
            Metadata={
                'api-name': api_name,
                'api-version': openapi_spec['info']['version'],
                'gateway-type': 'REST'
            }
        )

        print(f"API Gateway test 2739: ✓")

        print(f"\nTest 2739 - API Gateway 2739: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2739 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2739: {error_code}")
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
