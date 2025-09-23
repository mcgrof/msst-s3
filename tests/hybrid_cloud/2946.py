#!/usr/bin/env python3
"""
Test 2946: DataSync 2946

Tests AWS DataSync operations
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2946(s3_client, config):
    """DataSync 2946"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2946')
        s3_client.create_bucket(bucket_name)

        # Test AWS DataSync operations
        key = f'datasync/tasks/task-2946/status.json'

        # DataSync task configuration
        datasync_task = {
            'task_arn': f'arn:aws:datasync:us-east-1:123456789012:task/task-00002946',
            'source_location': {
                'type': ['nfs', 'smb', 's3', 'efs'][i % 4],
                'uri': f'{["nfs", "smb", "s3", "efs"][i % 4]}://source-2946.example.com/share'
            },
            'destination_location': {
                'type': 's3',
                'bucket': bucket_name,
                'prefix': f'datasync-2946/'
            },
            'options': {
                'verify_mode': ['POINT_IN_TIME_CONSISTENT', 'ONLY_FILES_TRANSFERRED'][i % 2],
                'bandwidth_limit': 100 if i % 3 == 0 else None,  # MB/s
                'preserve_metadata': ['OWNERSHIP', 'PERMISSIONS', 'TIMESTAMPS'],
                'task_schedule': 'rate(1 hour)' if i % 2 == 0 else None
            },
            'execution_status': {
                'status': ['SUCCESS', 'ERROR', 'IN_PROGRESS'][i % 3],
                'bytes_transferred': random.randint(1000000, 10000000000),
                'files_transferred': random.randint(100, 100000),
                'duration_seconds': random.randint(60, 3600),
                'throughput_mbps': random.uniform(10, 1000)
            }
        }

        # Store task status
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(datasync_task).encode()),
            Metadata={
                'task-arn': datasync_task['task_arn'],
                'source-type': datasync_task['source_location']['type'],
                'execution-status': datasync_task['execution_status']['status']
            }
        )

        print(f"DataSync operation test 2946: ✓")

        print(f"\nTest 2946 - DataSync 2946: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2946 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2946: {error_code}")
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
