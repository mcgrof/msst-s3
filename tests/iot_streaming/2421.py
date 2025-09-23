#!/usr/bin/env python3
"""
Test 2421: Firehose delivery 2421

Tests Kinesis Firehose delivery pattern
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2421(s3_client, config):
    """Firehose delivery 2421"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2421')
        s3_client.create_bucket(bucket_name)

        # Test Kinesis Firehose delivery simulation
        stream_name = f'kinesis-stream-1'
        batch_id = f'BATCH-0000002421'

        # Simulate batched streaming records
        records = []
        for r in range(10):  # 10 records per batch
            record = {
                'record_id': f'REC-00002421-{r:03d}',
                'data': {
                    'event_type': 'user_activity',
                    'user_id': f'USR-9628',
                    'action': ['click', 'view', 'purchase', 'scroll'][r % 4],
                    'timestamp': time.time() + r
                },
                'approximate_arrival_timestamp': time.time()
            }
            records.append(record)

        # Store as compressed batch
        batch_key = f'streaming/firehose/{stream_name}/{batch_id}.json.gz'

        import gzip
        batch_data = gzip.compress(json.dumps(records).encode())

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=batch_key,
            Body=io.BytesIO(batch_data),
            ContentEncoding='gzip',
            ContentType='application/json',
            Metadata={
                'stream-name': stream_name,
                'batch-id': batch_id,
                'record-count': str(len(records)),
                'compression': 'gzip'
            }
        )

        print(f"Firehose delivery test 2421: ✓")

        print(f"\nTest 2421 - Firehose delivery 2421: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2421 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2421: {error_code}")
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
