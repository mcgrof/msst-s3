#!/usr/bin/env python3
"""
Test 1600: Analytics/Inventory 1600

Tests storage analytics and inventory configuration
"""

import io
import time
import hashlib
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_1600(s3_client, config):
    """Analytics/Inventory 1600"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-1600')
        s3_client.create_bucket(bucket_name)

        # Test storage analytics and inventory
        try:
            if 1600 % 2 == 0:
                # Storage analytics configuration
                analytics_config = {
                    'Id': f'AnalyticsConfig1600',
                    'Filter': {
                        'Prefix': 'analytics/'
                    },
                    'StorageClassAnalysis': {
                        'DataExport': {
                            'OutputSchemaVersion': 'V_1',
                            'Destination': {
                                'S3BucketDestination': {
                                    'Format': 'CSV',
                                    'BucketAccountId': '123456789012',
                                    'Bucket': f'arn:aws:s3:::analytics-results-1600',
                                    'Prefix': 'results/'
                                }
                            }
                        }
                    }
                }

                s3_client.client.put_bucket_analytics_configuration(
                    Bucket=bucket_name,
                    Id=f'AnalyticsConfig1600',
                    AnalyticsConfiguration=analytics_config
                )
                print(f"Analytics configuration 1600 created")
            else:
                # Inventory configuration
                inventory_config = {
                    'Id': f'InventoryConfig1600',
                    'IsEnabled': True,
                    'Destination': {
                        'S3BucketDestination': {
                            'Bucket': f'arn:aws:s3:::inventory-results-1600',
                            'Format': 'CSV',
                            'Prefix': 'inventory/'
                        }
                    },
                    'Schedule': {
                        'Frequency': 'Daily'
                    },
                    'IncludedObjectVersions': 'All',
                    'OptionalFields': ['Size', 'LastModifiedDate', 'StorageClass']
                }

                s3_client.client.put_bucket_inventory_configuration(
                    Bucket=bucket_name,
                    Id=f'InventoryConfig1600',
                    InventoryConfiguration=inventory_config
                )
                print(f"Inventory configuration 1600 created")

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidRequest']:
                print(f"Analytics/Inventory feature not supported")
            else:
                raise

        print(f"\nTest 1600 - Analytics/Inventory 1600: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest']:
            print(f"Test 1600 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 1600: {error_code}")
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
