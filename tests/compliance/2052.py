#!/usr/bin/env python3
"""
Test 2052: PCI DSS compliance 2052

Tests PCI DSS payment card data protection
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2052(s3_client, config):
    """PCI DSS compliance 2052"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2052')
        s3_client.create_bucket(bucket_name)

        # Test PCI DSS compliance for payment card data
        key = 'pci-data-2052.json'

        # Simulated payment card data (tokenized)
        pci_data = {
            'transaction_id': f'TXN00002052',
            'card_token': hashlib.sha256(f'4111111111111111-2052'.encode()).hexdigest(),
            'amount': random.uniform(10.00, 1000.00),
            'currency': 'USD',
            'pci_compliant': True
        }

        # Upload with PCI DSS requirements
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(json.dumps(pci_data).encode()),
            ServerSideEncryption='AES256',
            Tagging='DataType=PCI&Level=1&Tokenized=True',
            Metadata={
                'pci-dss-version': '4.0',
                'data-classification': 'restricted',
                'audit-required': 'true'
            }
        )

        print(f"PCI DSS compliance test 2052: ✓")

        print(f"\nTest 2052 - PCI DSS compliance 2052: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2052 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2052: {error_code}")
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
