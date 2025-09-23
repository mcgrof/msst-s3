#!/usr/bin/env python3
"""
Test 2226: Data partitioning 2226

Tests time-based data partitioning
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2226(s3_client, config):
    """Data partitioning 2226"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2226')
        s3_client.create_bucket(bucket_name)

        # Test data lake partitioning strategies
        year = 2020 + (i % 5)
        month = (i % 12) + 1
        day = (i % 28) + 1
        hour = i % 24

        # Create partitioned data structure
        partition_key = f'data-lake/events/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data-2226.json'

        event_data = {
            'event_id': f'EVT000000002226',
            'timestamp': f'{year}-{month:02d}-{day:02d}T{hour:02d}:00:00Z',
            'event_type': ['click', 'view', 'purchase', 'signup'][i % 4],
            'user_id': f'USR002226',
            'value': random.uniform(0.01, 1000.00)
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=partition_key,
            Body=io.BytesIO(json.dumps(event_data).encode()),
            Metadata={
                'partition-year': str(year),
                'partition-month': str(month),
                'partition-day': str(day),
                'partition-hour': str(hour),
                'partition-strategy': 'time-based'
            }
        )

        print(f"Data partitioning test 2226: ✓")

        print(f"\nTest 2226 - Data partitioning 2226: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2226 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2226: {error_code}")
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
