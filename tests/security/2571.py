#!/usr/bin/env python3
"""
Test 2571: Audit logging 2571

Tests audit logging and monitoring
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2571(s3_client, config):
    """Audit logging 2571"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2571')
        s3_client.create_bucket(bucket_name)

        # Test audit logging
        key = f'audit-logs/log-2571.json'

        # Audit event
        audit_event = {
            'event_id': f'AUDIT-000000002571',
            'timestamp': time.time(),
            'event_type': ['ObjectCreated', 'ObjectRead', 'ObjectDeleted', 'BucketModified'][i % 4],
            'user': {
                'id': f'user-71',
                'ip_address': f'192.168.11.11',
                'user_agent': 'aws-cli/2.0.0'
            },
            'resource': {
                'type': 's3_object',
                'bucket': bucket_name,
                'key': f'resource-2571.dat'
            },
            'action': {
                'operation': ['PUT', 'GET', 'DELETE', 'POST'][i % 4],
                'status': 'success',
                'duration_ms': random.randint(10, 1000)
            },
            'security': {
                'encryption': 'AES256',
                'signed_request': True,
                'mfa_used': i % 3 == 0
            }
        }

        # Store audit log
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(audit_event).encode()),
            ServerSideEncryption='AES256',
            Metadata={
                'audit-event-id': audit_event['event_id'],
                'event-type': audit_event['event_type'],
                'user-id': audit_event['user']['id']
            }
        )

        print(f"Audit logging test 2571: ✓")

        print(f"\nTest 2571 - Audit logging 2571: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2571 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2571: {error_code}")
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
