#!/usr/bin/env python3
"""
Test 2254: Iceberg format 2254

Tests Apache Iceberg table format
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2254(s3_client, config):
    """Iceberg format 2254"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2254')
        s3_client.create_bucket(bucket_name)

        # Test Apache Iceberg table format
        key = f'data-lake/iceberg/table-2254/metadata/v{i}.metadata.json'

        # Iceberg table metadata
        iceberg_metadata = {
            'format-version': 2,
            'table-uuid': f'table-2254-' + 'x' * 32,
            'location': f's3://{bucket_name}/data-lake/iceberg/table-2254',
            'schema': {
                'type': 'struct',
                'fields': [
                    {'id': 1, 'name': 'id', 'type': 'long'},
                    {'id': 2, 'name': 'data', 'type': 'string'},
                    {'id': 3, 'name': 'ts', 'type': 'timestamp'}
                ]
            },
            'partition-spec': [],
            'properties': {
                'write.format.default': 'parquet',
                'write.parquet.compression': 'snappy'
            }
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(iceberg_metadata).encode()),
            ContentType='application/json',
            Metadata={
                'table-format': 'iceberg',
                'format-version': '2',
                'table-name': f'table_2254'
            }
        )

        print(f"Iceberg table test 2254: ✓")

        print(f"\nTest 2254 - Iceberg format 2254: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2254 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2254: {error_code}")
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
