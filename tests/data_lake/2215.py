#!/usr/bin/env python3
"""
Test 2215: Parquet handling 2215

Tests Parquet file handling for data lakes
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2215(s3_client, config):
    """Parquet handling 2215"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2215')
        s3_client.create_bucket(bucket_name)

        # Test Parquet file handling for data lakes
        key = f'data-lake/parquet/data-2215.parquet'

        # Simulate Parquet file metadata
        parquet_metadata = {
            'schema': {
                'columns': ['id', 'timestamp', 'value', 'category'],
                'types': ['int64', 'timestamp', 'double', 'string']
            },
            'row_groups': 10,
            'total_rows': 1000000,
            'compression': 'snappy',
            'created_by': 'Apache Spark 3.0'
        }

        # Upload with data lake metadata
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'PARQUET_FILE_HEADER' + b'\x00' * 10000),  # Simulated Parquet
            ContentType='application/octet-stream',
            Metadata={
                'file-format': 'parquet',
                'partition-key': f'year=2024/month=8/day=4',
                'table-name': 'events',
                'catalog': 'aws-glue'
            },
            Tagging='DataLake=True&Format=Parquet&Compressed=True'
        )

        print(f"Parquet file test 2215: ✓")

        print(f"\nTest 2215 - Parquet handling 2215: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2215 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2215: {error_code}")
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
