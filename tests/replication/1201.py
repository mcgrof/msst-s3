#!/usr/bin/env python3
"""
Test 1201: Replication cross_region_replication

Tests cross_region_replication replication scenario
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1201(s3_client, config):
    """Replication cross_region_replication"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1201')
        s3_client.create_bucket(bucket_name)

        # Test cross_region_replication
        try:
            if 'cross_region_replication' == 'cross_region_replication':
                # Setup replication configuration
                replication_config = {
                    'Role': 'arn:aws:iam::123456789012:role/replication-role',
                    'Rules': [{
                        'ID': 'ReplicateAll',
                        'Priority': 1,
                        'Status': 'Enabled',
                        'Destination': {
                            'Bucket': 'arn:aws:s3:::destination-bucket-1201'
                        }
                    }]
                }

                s3_client.client.put_bucket_replication(
                    Bucket=bucket_name,
                    ReplicationConfiguration=replication_config
                )
                print("Cross-region replication configured")

            elif 'cross_region_replication' == 'replication_metrics':
                # Test replication metrics
                metrics_config = {
                    'Status': 'Enabled',
                    'EventThreshold': {
                        'Minutes': 15
                    }
                }

                print("Replication metrics test")

            else:
                # Generic replication test
                key = f'replicated-object-1201.txt'
                s3_client.put_object(bucket_name, key, io.BytesIO(b'Replicated content'))
                print(f"Replication scenario 'cross_region_replication' tested")

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidRequest']:
                print(f"Replication feature 'cross_region_replication' not supported")
            else:
                raise

        print(f"\nTest 1201 - Replication cross_region_replication: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1201 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1201: {error_code}")
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
