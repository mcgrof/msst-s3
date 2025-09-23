#!/usr/bin/env python3
"""
Test 2413: IoT ingestion 2413

Tests IoT device data ingestion
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2413(s3_client, config):
    """IoT ingestion 2413"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2413')
        s3_client.create_bucket(bucket_name)

        # Test IoT device data ingestion
        device_id = f'IOT-DEVICE-00002413'
        timestamp = int(time.time() * 1000)

        # IoT telemetry data
        telemetry = {
            'device_id': device_id,
            'timestamp': timestamp,
            'location': {
                'lat': 37.7749 + random.uniform(-1, 1),
                'lon': -122.4194 + random.uniform(-1, 1)
            },
            'sensors': {
                'temperature': 20 + random.uniform(-10, 20),
                'humidity': 50 + random.uniform(-30, 30),
                'pressure': 1013 + random.uniform(-20, 20),
                'battery': 80 + random.uniform(-30, 20)
            },
            'status': ['online', 'idle', 'active'][i % 3]
        }

        # Store with time-series partitioning
        key = f'iot-data/devices/{device_id}/year={timestamp // (365*24*3600*1000) + 1970}/month={(timestamp // (30*24*3600*1000)) % 12 + 1:02d}/data-{timestamp}.json'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(telemetry).encode()),
            ContentType='application/json',
            Metadata={
                'device-id': device_id,
                'data-type': 'telemetry',
                'ingestion-time': str(timestamp),
                'partition-key': device_id
            }
        )

        print(f"IoT ingestion test 2413: ✓")

        print(f"\nTest 2413 - IoT ingestion 2413: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2413 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2413: {error_code}")
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
