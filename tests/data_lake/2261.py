#!/usr/bin/env python3
"""
Test 2261: Delta Lake 2261

Tests Delta Lake table format
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2261(s3_client, config):
    """Delta Lake 2261"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2261')
        s3_client.create_bucket(bucket_name)

        # Test Delta Lake format
        delta_path = f'data-lake/delta/table-2261/'

        # Delta log entry
        log_key = f'{delta_path}_delta_log/00000000000000000{i % 100:03d}.json'

        delta_log = {
            'commitInfo': {
                'timestamp': int(time.time() * 1000),
                'operation': 'WRITE',
                'operationParameters': {'mode': 'Append'},
                'version': i % 100
            },
            'add': {
                'path': f'part-{i:05d}-xxx.parquet',
                'size': 1024 * 1024 * random.randint(1, 100),
                'partitionValues': {},
                'dataChange': True,
                'stats': '{"numRecords": 10000}'
            }
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=log_key,
            Body=io.BytesIO(json.dumps(delta_log).encode()),
            ContentType='application/json',
            Metadata={
                'table-format': 'delta',
                'transaction-version': str(i % 100)
            }
        )

        print(f"Delta Lake test 2261: ✓")

        print(f"\nTest 2261 - Delta Lake 2261: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2261 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2261: {error_code}")
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
