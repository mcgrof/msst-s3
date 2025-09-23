#!/usr/bin/env python3
"""
Test 2667: Global Accelerator 2667

Tests AWS Global Accelerator patterns
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2667(s3_client, config):
    """Global Accelerator 2667"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2667')
        s3_client.create_bucket(bucket_name)

        # Test Global Accelerator patterns
        key = f'global-accelerator/endpoint-2667.json'

        # Global Accelerator configuration
        accelerator_config = {
            'accelerator_id': f'ACC-002667',
            'listeners': [
                {'port': 80, 'protocol': 'TCP'},
                {'port': 443, 'protocol': 'TCP'}
            ],
            'endpoint_groups': [
                {
                    'region': 'us-east-1',
                    'endpoints': [f'endpoint-{j}.example.com' for j in range(2)],
                    'traffic_dial': 100,
                    'health_check_interval': 30
                },
                {
                    'region': 'eu-west-1',
                    'endpoints': [f'endpoint-eu-{j}.example.com' for j in range(2)],
                    'traffic_dial': 50,
                    'health_check_interval': 30
                }
            ],
            'client_affinity': 'SOURCE_IP',
            'anycast_ips': [f'192.0.2.107', f'198.51.100.107']
        }

        # Store accelerator config
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(accelerator_config).encode()),
            Metadata={
                'accelerator-id': accelerator_config['accelerator_id'],
                'endpoint-count': str(sum(len(eg['endpoints']) for eg in accelerator_config['endpoint_groups'])),
                'client-affinity': accelerator_config['client_affinity']
            }
        )

        print(f"Global Accelerator test 2667: ✓")

        print(f"\nTest 2667 - Global Accelerator 2667: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2667 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2667: {error_code}")
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
