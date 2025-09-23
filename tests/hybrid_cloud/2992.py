#!/usr/bin/env python3
"""
Test 2992: Outposts 2992

Tests AWS Outposts S3 configuration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2992(s3_client, config):
    """Outposts 2992"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2992')
        s3_client.create_bucket(bucket_name)

        # Test AWS Outposts configuration
        key = f'outposts/outpost-2992/config.json'

        # Outposts configuration
        outpost_config = {
            'outpost_id': f'op-000000000bb0',
            'site_id': f'site-00000002',
            'availability_zone': f'us-east-1-op-2a',
            'capacity': {
                'compute': {
                    'ec2_instances': {
                        'm5.large': 10 * (i % 5 + 1),
                        'm5.xlarge': 5 * (i % 3 + 1),
                        'm5.2xlarge': 2 * (i % 2 + 1)
                    }
                },
                'storage': {
                    'ebs_volume_gb': 1000 * (i % 100 + 1),
                    's3_capacity_gb': 10000 * (i % 10 + 1)
                },
                'network': {
                    'local_gateway': f'lgw-00000bb0',
                    'bandwidth_gbps': [10, 40, 100][i % 3]
                }
            },
            's3_on_outposts': {
                'buckets': [
                    {
                        'name': f'outpost-bucket-2992-{j}',
                        'capacity_gb': 1000,
                        'storage_class': 'OUTPOSTS'
                    } for j in range(3)
                ],
                'access_points': [f'ap-outpost-2992-{j}' for j in range(2)],
                'endpoints': [f'https://op-000000000bb0.s3-outposts.amazonaws.com']
            },
            'connectivity': {
                'service_link': 'ENABLED',
                'local_network': '10.0.0.0/8',
                'region_connection': 'direct-connect'
            }
        }

        # Store Outposts configuration
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(outpost_config).encode()),
            Metadata={
                'outpost-id': outpost_config['outpost_id'],
                'site-id': outpost_config['site_id'],
                's3-on-outposts': 'enabled',
                's3-capacity-gb': str(outpost_config['capacity']['storage']['s3_capacity_gb'])
            }
        )

        print(f"AWS Outposts test 2992: ✓")

        print(f"\nTest 2992 - Outposts 2992: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2992 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2992: {error_code}")
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
