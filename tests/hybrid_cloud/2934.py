#!/usr/bin/env python3
"""
Test 2934: On-premises 2934

Tests on-premises storage gateway integration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2934(s3_client, config):
    """On-premises 2934"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2934')
        s3_client.create_bucket(bucket_name)

        # Test on-premises integration
        key = f'hybrid/on-prem/gateway-2934/config.json'

        # Storage Gateway configuration
        gateway_config = {
            'gateway_type': ['file', 'volume', 'tape'][i % 3],
            'gateway_id': f'SGW-00002934',
            'on_premises': {
                'location': f'datacenter-4',
                'ip_address': f'10.118.118.118',
                'bandwidth_mbps': 100 * (i % 10 + 1),
                'cache_size_gb': 100 * (i % 50 + 1)
            },
            'cloud_connection': {
                'endpoint': f's3.amazonaws.com',
                'bucket': bucket_name,
                'prefix': f'on-prem-2934/',
                'upload_buffer_gb': 100,
                'upload_schedule': 'continuous'
            },
            'nfs_exports': [
                {
                    'export_path': f'/mnt/share{j}',
                    'client_list': ['10.0.0.0/8'],
                    'squash': 'none'
                } for j in range(3)
            ],
            'monitoring': {
                'cloudwatch_enabled': True,
                'metrics_interval': 60,
                'log_group': f'/aws/storage-gateway/{i}'
            }
        }

        # Store gateway configuration
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(gateway_config).encode()),
            Metadata={
                'gateway-id': gateway_config['gateway_id'],
                'gateway-type': gateway_config['gateway_type'],
                'on-premises': 'true',
                'cache-size-gb': str(gateway_config['on_premises']['cache_size_gb'])
            }
        )

        print(f"On-premises integration test 2934: ✓")

        print(f"\nTest 2934 - On-premises 2934: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2934 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2934: {error_code}")
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
