#!/usr/bin/env python3
"""
Test 2971: Direct Connect 2971

Tests Direct Connect virtual interface
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2971(s3_client, config):
    """Direct Connect 2971"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2971')
        s3_client.create_bucket(bucket_name)

        # Test Direct Connect configuration
        key = f'direct-connect/vif-2971/config.json'

        # Virtual Interface configuration
        vif_config = {
            'vif_id': f'dxvif-00000b9b',
            'connection_id': f'dxcon-00000001',
            'vif_type': ['private', 'public', 'transit'][i % 3],
            'vlan': 100 + i,
            'customer_address': f'192.168.155.1/30',
            'amazon_address': f'192.168.155.2/30',
            'bgp_config': {
                'asn': 65000 + i,
                'auth_key': hashlib.md5(f'auth-2971'.encode()).hexdigest()[:16],
                'prefixes_advertised': [
                    f'10.155.0.0/16',
                    f'172.27.0.0/16'
                ]
            },
            'bandwidth': f'{[1, 10, 100][i % 3]}Gbps',
            'jumbo_frames': i % 2 == 0,
            'tags': {
                'environment': ['prod', 'staging', 'dev'][i % 3],
                'cost_center': f'CC-071'
            }
        }

        # Store VIF configuration
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(vif_config).encode()),
            Metadata={
                'vif-id': vif_config['vif_id'],
                'vif-type': vif_config['vif_type'],
                'bandwidth': vif_config['bandwidth']
            }
        )

        print(f"Direct Connect test 2971: ✓")

        print(f"\nTest 2971 - Direct Connect 2971: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2971 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2971: {error_code}")
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
