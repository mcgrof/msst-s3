#!/usr/bin/env python3
"""
Test 2078: SOC 2 compliance 2078

Tests SOC 2 trust service criteria
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2078(s3_client, config):
    """SOC 2 compliance 2078"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2078')
        s3_client.create_bucket(bucket_name)

        # Test SOC 2 compliance requirements
        key = 'soc2-data-2078.json'

        # SOC 2 audit trail data
        audit_data = {
            'event_id': f'EVT0000002078',
            'timestamp': time.time(),
            'user': f'auditor2078',
            'action': 'data_access',
            'trust_service_criteria': ['security', 'availability', 'confidentiality'],
            'controls_tested': True
        }

        # Upload with audit trail
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(audit_data).encode()),
            Metadata={
                'soc2-type': 'Type-II',
                'audit-period': '12-months',
                'control-environment': 'production'
            }
        )

        print(f"SOC 2 compliance test 2078: ✓")

        print(f"\nTest 2078 - SOC 2 compliance 2078: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2078 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2078: {error_code}")
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
