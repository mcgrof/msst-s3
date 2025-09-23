#!/usr/bin/env python3
"""
Test 2708: Service mesh 2708

Tests service mesh configuration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2708(s3_client, config):
    """Service mesh 2708"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2708')
        s3_client.create_bucket(bucket_name)

        # Test service mesh configuration
        service_name = f'service-api-2708'
        key = f'service-mesh/{service_name}/config.yaml'

        # Service mesh configuration
        mesh_config = {
            'service': {
                'name': service_name,
                'version': f'v4.8.8',
                'namespace': 'production',
                'replicas': i % 10 + 1
            },
            'sidecar': {
                'enabled': True,
                'proxy': 'envoy',
                'version': '1.21.0'
            },
            'traffic_management': {
                'load_balancing': ['round_robin', 'least_request', 'random'][i % 3],
                'circuit_breaker': {
                    'consecutive_errors': 5,
                    'interval': 30,
                    'base_ejection_time': 30
                },
                'retry_policy': {
                    'attempts': 3,
                    'timeout': 5000,
                    'retry_on': ['5xx', 'reset', 'refused']
                }
            },
            'observability': {
                'metrics': True,
                'tracing': True,
                'access_logs': True
            }
        }

        # Store service mesh config
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(mesh_config).encode()),
            ContentType='application/yaml',
            Metadata={
                'service-name': service_name,
                'mesh-enabled': 'true',
                'version': mesh_config['service']['version']
            }
        )

        print(f"Service mesh test 2708: ✓")

        print(f"\nTest 2708 - Service mesh 2708: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2708 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2708: {error_code}")
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
