#!/usr/bin/env python3
"""
Test 2756: Event-driven 2756

Tests event-driven architecture patterns
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2756(s3_client, config):
    """Event-driven 2756"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2756')
        s3_client.create_bucket(bucket_name)

        # Test event-driven patterns
        event_type = ['order.created', 'user.registered', 'payment.processed', 'item.shipped'][i % 4]
        key = f'events/{event_type.replace(".", "/")}/event-2756.json'

        # Event message
        event = {
            'event_id': f'EVT-000000002756',
            'event_type': event_type,
            'timestamp': time.time(),
            'version': '2.0',
            'source': f'service-6',
            'correlation_id': f'CORR-00002756',
            'data': {
                'entity_id': f'ENT-002756',
                'attributes': {f'attr_{j}': f'value_{j}' for j in range(5)},
                'metadata': {
                    'user_id': f'USR-0756',
                    'tenant_id': f'TNT-056'
                }
            },
            'routing_key': f'{event_type}.high'
        }

        # Store event
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(event).encode()),
            ContentType='application/json',
            Metadata={
                'event-id': event['event_id'],
                'event-type': event_type,
                'correlation-id': event['correlation_id'],
                'routing-key': event['routing_key']
            }
        )

        print(f"Event-driven test 2756: ✓")

        print(f"\nTest 2756 - Event-driven 2756: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2756 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2756: {error_code}")
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
