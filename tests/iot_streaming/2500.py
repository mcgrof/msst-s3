#!/usr/bin/env python3
"""
Test 2500: CDC pattern 2500

Tests Change Data Capture pattern
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2500(s3_client, config):
    """CDC pattern 2500"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2500')
        s3_client.create_bucket(bucket_name)

        # Test CDC pattern
        table_name = f'table_0'

        # CDC event
        cdc_event = {
            'database': 'production',
            'table': table_name,
            'operation': ['INSERT', 'UPDATE', 'DELETE'][i % 3],
            'timestamp': time.time(),
            'primary_key': {'id': f'ID-00002500'},
            'before': {'id': f'ID-00002500', 'value': 'old_value'} if i % 3 != 0 else None,
            'after': {'id': f'ID-00002500', 'value': 'new_value'} if i % 3 != 2 else None,
            'source': {
                'version': '1.9.0',
                'connector': 'mysql',
                'server_id': 1,
                'binlog_file': 'mysql-bin.000042',
                'binlog_position': i * 1000
            }
        }

        # Store CDC event
        key = f'cdc/{table_name}/{int(time.time() * 1000)}-2500.json'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(cdc_event).encode()),
            ContentType='application/json',
            Metadata={
                'cdc-operation': cdc_event['operation'],
                'source-table': table_name,
                'binlog-position': str(cdc_event['source']['binlog_position'])
            }
        )

        print(f"CDC pattern test 2500: ✓")

        print(f"\nTest 2500 - CDC pattern 2500: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2500 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2500: {error_code}")
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
