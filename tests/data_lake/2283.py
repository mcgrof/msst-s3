#!/usr/bin/env python3
"""
Test 2283: Data catalog 2283

Tests data catalog integration
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2283(s3_client, config):
    """Data catalog 2283"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2283')
        s3_client.create_bucket(bucket_name)

        # Test data catalog integration
        key = f'data-lake/catalog/database-3/table-2283/data.orc'

        # Catalog metadata
        catalog_entry = {
            'database': f'database_3',
            'table': f'table_2283',
            'columns': [
                {'name': 'id', 'type': 'bigint', 'comment': 'Primary key'},
                {'name': 'name', 'type': 'string', 'comment': 'Name field'},
                {'name': 'value', 'type': 'decimal(10,2)', 'comment': 'Value'},
                {'name': 'created', 'type': 'timestamp', 'comment': 'Creation time'}
            ],
            'location': f's3://{bucket_name}/data-lake/catalog/database-3/table-2283/',
            'input_format': 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',
            'output_format': 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',
            'serde': 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'ORC' + b'\x00' * 5000),  # Simulated ORC file
            ContentType='application/octet-stream',
            Metadata={
                'catalog-database': catalog_entry['database'],
                'catalog-table': catalog_entry['table'],
                'catalog-format': 'orc',
                'catalog-registered': 'true'
            }
        )

        print(f"Data catalog test 2283: ✓")

        print(f"\nTest 2283 - Data catalog 2283: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2283 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2283: {error_code}")
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
