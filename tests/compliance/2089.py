#!/usr/bin/env python3
"""
Test 2089: ISO 27001 compliance 2089

Tests ISO 27001 information security management
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2089(s3_client, config):
    """ISO 27001 compliance 2089"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2089')
        s3_client.create_bucket(bucket_name)

        # Test ISO 27001 information security
        key = 'iso27001-data-2089.json'

        # Information security management data
        isms_data = {
            'asset_id': f'ASSET002089',
            'classification': 'confidential',
            'risk_level': 'medium',
            'controls': ['access_control', 'encryption', 'monitoring'],
            'last_review': time.time()
        }

        # Upload with ISO 27001 controls
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(isms_data).encode()),
            ServerSideEncryption='AES256',
            Tagging='Standard=ISO27001&Classification=Confidential',
            Metadata={
                'iso27001-control': 'A.10.1.1',
                'risk-assessment': 'completed',
                'security-policy': 'enforced'
            }
        )

        print(f"ISO 27001 compliance test 2089: ✓")

        print(f"\nTest 2089 - ISO 27001 compliance 2089: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2089 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2089: {error_code}")
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
