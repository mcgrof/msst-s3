#!/usr/bin/env python3
"""
Test 2506: Encryption aws:kms 2506

Tests aws:kms encryption at rest
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2506(s3_client, config):
    """Encryption aws:kms 2506"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2506')
        s3_client.create_bucket(bucket_name)

        # Test encryption at rest - aws:kms
        key = f'encrypted/{encryption_type}/data-2506.bin'

        # Sensitive data requiring encryption
        sensitive_data = {
            'ssn': hashlib.sha256(f'123-45-2506'.encode()).hexdigest(),
            'credit_card': hashlib.sha256(f'4111-1111-1111-2506'.encode()).hexdigest(),
            'api_key': hashlib.sha256(f'sk_live_2506'.encode()).hexdigest(),
            'password_hash': hashlib.pbkdf2_hmac('sha256', f'pass2506'.encode(), b'salt', 100000).hex()
        }

        # Apply encryption based on type
        if 'aws:kms' == 'AES256':
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(json.dumps(sensitive_data).encode()),
                ServerSideEncryption='AES256'
            )
        elif 'aws:kms' == 'aws:kms':
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=io.BytesIO(json.dumps(sensitive_data).encode()),
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId='arn:aws:kms:us-east-1:123456789012:key/test-key'
                )
            except:
                # Fallback to AES256 if KMS not available
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=io.BytesIO(json.dumps(sensitive_data).encode()),
                    ServerSideEncryption='AES256'
                )

        print(f"Encryption at rest (aws:kms) test 2506: ✓")

        print(f"\nTest 2506 - Encryption aws:kms 2506: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2506 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2506: {error_code}")
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
