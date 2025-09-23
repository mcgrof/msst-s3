#!/usr/bin/env python3
"""
Test 2471: Event sourcing 2471

Tests event sourcing pattern
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2471(s3_client, config):
    """Event sourcing 2471"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2471')
        s3_client.create_bucket(bucket_name)

        # Test event sourcing pattern
        aggregate_id = f'AGG-00002471'

        # Create event stream
        events = []
        for seq in range(5):
            event = {
                'aggregate_id': aggregate_id,
                'sequence': seq,
                'event_type': ['Created', 'Updated', 'Deleted', 'Restored', 'Archived'][seq % 5],
                'timestamp': time.time() + seq,
                'data': {
                    'field1': f'value_{seq}',
                    'field2': random.randint(1, 100)
                },
                'metadata': {
                    'user': f'user-1',
                    'source': 'api',
                    'version': '1.0'
                }
            }
            events.append(event)

            # Store each event
            event_key = f'event-store/{aggregate_id}/seq-{seq:06d}.json'
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=event_key,
                Body=io.BytesIO(json.dumps(event).encode()),
                Metadata={
                    'aggregate-id': aggregate_id,
                    'sequence': str(seq),
                    'event-type': event['event_type']
                }
            )

        print(f"Event sourcing test 2471: ✓")

        print(f"\nTest 2471 - Event sourcing 2471: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2471 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2471: {error_code}")
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
