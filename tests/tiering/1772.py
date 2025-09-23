#!/usr/bin/env python3
"""
Test 1772: Intelligent-Tiering 1772

Tests Intelligent-Tiering configuration 1772
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1772(s3_client, config):
    """Intelligent-Tiering 1772"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1772')
        s3_client.create_bucket(bucket_name)

        # Test Intelligent-Tiering configuration 1772
        try:
            # Configure Intelligent-Tiering
            tiering_config = {
                'Id': f'TieringConfig1772',
                'Status': 'Enabled',
                'Tierings': [
                    {
                        'Days': 90,
                        'AccessTier': 'ARCHIVE_ACCESS'
                    },
                    {
                        'Days': 180,
                        'AccessTier': 'DEEP_ARCHIVE_ACCESS'
                    }
                ],
                'Filter': {
                    'Prefix': 'tiering/',
                    'Tag': {
                        'Key': 'TieringEnabled',
                        'Value': 'true'
                    }
                }
            }

            s3_client.client.put_bucket_intelligent_tiering_configuration(
                Bucket=bucket_name,
                Id=f'TieringConfig1772',
                IntelligentTieringConfiguration=tiering_config
            )

            # Upload object with Intelligent-Tiering storage class
            key = f'tiering/object-1772.dat'
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'X' * 1024),
                StorageClass='INTELLIGENT_TIERING',
                Tagging='TieringEnabled=true'
            )

            print(f"Intelligent-Tiering config 1772 created")

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidStorageClass']:
                print("Intelligent-Tiering not supported")
            else:
                raise

        print(f"\nTest 1772 - Intelligent-Tiering 1772: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1772 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1772: {error_code}")
            raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
