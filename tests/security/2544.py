#!/usr/bin/env python3
"""
Test 2544: Data masking 2544

Tests data masking and tokenization
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2544(s3_client, config):
    """Data masking 2544"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2544')
        s3_client.create_bucket(bucket_name)

        # Test data masking and tokenization
        key = f'masked-data/record-2544.json'

        # Original sensitive data
        original = {
            'customer_id': f'CUST-00002544',
            'email': f'user2544@example.com',
            'phone': f'+1-555-2544',
            'ssn': f'844-34-3544',
            'dob': f'1994-01-25'
        }

        # Apply masking
        masked = {
            'customer_id': original['customer_id'],  # Keep ID
            'email': f'***2544@***.com',  # Partial mask
            'phone': f'+1-555-XXXX',  # Full mask
            'ssn': f'XXX-XX-{original["ssn"][-4:]}',  # Last 4 only
            'dob': f'{original["dob"][:4]}-XX-XX',  # Year only
            'token_map': hashlib.sha256(json.dumps(original).encode()).hexdigest()
        }

        # Store masked data
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(masked).encode()),
            ServerSideEncryption='AES256',
            Metadata={
                'data-masked': 'true',
                'masking-level': 'partial',
                'token-id': masked['token_map'][:8]
            }
        )

        print(f"Data masking test 2544: ✓")

        print(f"\nTest 2544 - Data masking 2544: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2544 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2544: {error_code}")
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
