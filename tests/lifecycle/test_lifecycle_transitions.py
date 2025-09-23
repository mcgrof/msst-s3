#!/usr/bin/env python3
"""
Test: Lifecycle Rule Transitions and Expirations
Tests lifecycle configuration validation, transition rules, and expiration handling
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
from datetime import datetime, timedelta
import io

def test_lifecycle_transitions(s3_client: S3Client):
    """Test lifecycle transitions and expirations"""
    bucket_name = f's3-lifecycle-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic lifecycle configuration
        print("Test 1: Basic lifecycle configuration")
        basic_lifecycle = {
            'Rules': [
                {
                    'ID': 'basic-rule',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'logs/'},
                    'Expiration': {'Days': 30},
                    'Transitions': [
                        {
                            'Days': 7,
                            'StorageClass': 'GLACIER'
                        }
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=basic_lifecycle
            )

            # Retrieve and verify
            config = s3_client.client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            if len(config['Rules']) == 1 and config['Rules'][0]['ID'] == 'basic-rule':
                results['passed'].append('Basic lifecycle configuration')
                print("✓ Basic lifecycle: Configuration set")
            else:
                results['failed'].append('Basic lifecycle: Configuration altered')

        except Exception as e:
            results['failed'].append(f'Basic lifecycle: {str(e)}')

        # Test 2: Multiple transition rules with ordering
        print("\nTest 2: Multiple transitions with ordering")
        multi_transition = {
            'Rules': [
                {
                    'ID': 'multi-transition',
                    'Status': 'Enabled',
                    'Filter': {},
                    'Transitions': [
                        {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                        {'Days': 60, 'StorageClass': 'GLACIER'},
                        {'Days': 180, 'StorageClass': 'DEEP_ARCHIVE'}
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=multi_transition
            )
            results['passed'].append('Multiple ordered transitions')
            print("✓ Multiple transitions: Correctly ordered")
        except Exception as e:
            results['failed'].append(f'Multiple transitions: {str(e)}')

        # Test 3: Invalid transition ordering (should fail)
        print("\nTest 3: Invalid transition ordering")
        invalid_order = {
            'Rules': [
                {
                    'ID': 'invalid-order',
                    'Status': 'Enabled',
                    'Filter': {},
                    'Transitions': [
                        {'Days': 60, 'StorageClass': 'GLACIER'},
                        {'Days': 30, 'StorageClass': 'STANDARD_IA'}  # Wrong order
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=invalid_order
            )
            results['failed'].append('Invalid transition order: Accepted')
            print("✗ Invalid transition order: Accepted (should reject)")
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'MalformedXML' in str(e):
                results['passed'].append('Invalid transition order rejected')
                print("✓ Invalid transition order: Correctly rejected")
            else:
                results['failed'].append(f'Invalid order: Unexpected error')

        # Test 4: Conflicting rules
        print("\nTest 4: Conflicting lifecycle rules")
        conflicting = {
            'Rules': [
                {
                    'ID': 'rule1',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'data/'},
                    'Expiration': {'Days': 30}
                },
                {
                    'ID': 'rule2',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'data/'},  # Same prefix
                    'Expiration': {'Days': 60}  # Different expiration
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=conflicting
            )
            # Some implementations allow this, applying both rules
            results['passed'].append('Conflicting rules handled')
            print("✓ Conflicting rules: Accepted (may apply both)")
        except Exception as e:
            if 'InvalidRequest' in str(e):
                results['passed'].append('Conflicting rules rejected')
                print("✓ Conflicting rules: Rejected for safety")
            else:
                results['failed'].append(f'Conflicting rules: {str(e)}')

        # Test 5: Tag-based filtering
        print("\nTest 5: Tag-based lifecycle filtering")
        tag_based = {
            'Rules': [
                {
                    'ID': 'tag-based',
                    'Status': 'Enabled',
                    'Filter': {
                        'And': {
                            'Prefix': 'tagged/',
                            'Tags': [
                                {'Key': 'Environment', 'Value': 'Production'},
                                {'Key': 'Archive', 'Value': 'true'}
                            ]
                        }
                    },
                    'Expiration': {'Days': 90}
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=tag_based
            )
            results['passed'].append('Tag-based filtering')
            print("✓ Tag-based filtering: Configuration accepted")
        except Exception as e:
            results['failed'].append(f'Tag-based filtering: {str(e)}')

        # Test 6: AbortIncompleteMultipartUpload
        print("\nTest 6: Abort incomplete multipart uploads")
        abort_multipart = {
            'Rules': [
                {
                    'ID': 'abort-incomplete',
                    'Status': 'Enabled',
                    'Filter': {},
                    'AbortIncompleteMultipartUpload': {
                        'DaysAfterInitiation': 7
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=abort_multipart
            )
            results['passed'].append('Abort incomplete multipart')
            print("✓ Abort incomplete multipart: Configuration set")
        except Exception as e:
            results['failed'].append(f'Abort multipart: {str(e)}')

        # Test 7: Date-based expiration
        print("\nTest 7: Date-based expiration")
        future_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%dT00:00:00Z')
        date_expiration = {
            'Rules': [
                {
                    'ID': 'date-expiration',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'expire/'},
                    'Expiration': {'Date': future_date}
                }
            ]
        }

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=date_expiration
            )
            results['passed'].append('Date-based expiration')
            print("✓ Date-based expiration: Configuration set")
        except Exception as e:
            results['failed'].append(f'Date expiration: {str(e)}')

        # Test 8: Maximum rules limit (typically 1000)
        print("\nTest 8: Maximum lifecycle rules")
        max_rules = {'Rules': []}
        for i in range(100):  # Test with 100 rules
            max_rules['Rules'].append({
                'ID': f'rule-{i:04d}',
                'Status': 'Enabled',
                'Filter': {'Prefix': f'prefix{i:04d}/'},
                'Expiration': {'Days': 365}
            })

        try:
            s3_client.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=max_rules
            )
            results['passed'].append('Many lifecycle rules (100)')
            print("✓ Many rules: 100 rules accepted")
        except Exception as e:
            if 'TooManyRules' in str(e):
                results['failed'].append(f'Max rules: Limit lower than 100')
            else:
                results['failed'].append(f'Max rules: {str(e)}')

        # Test 9: Delete lifecycle configuration
        print("\nTest 9: Delete lifecycle configuration")
        try:
            s3_client.client.delete_bucket_lifecycle(Bucket=bucket_name)

            # Verify deletion
            try:
                s3_client.client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                results['failed'].append('Delete lifecycle: Still exists')
            except Exception as e:
                if 'NoSuchLifecycleConfiguration' in str(e) or '404' in str(e):
                    results['passed'].append('Lifecycle deleted')
                    print("✓ Delete lifecycle: Successfully removed")
                else:
                    results['failed'].append('Delete lifecycle: Unexpected error')

        except Exception as e:
            results['failed'].append(f'Delete lifecycle: {str(e)}')

        # Summary
        print(f"\n=== Lifecycle Transitions Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            s3_client.delete_bucket(bucket_name)
        except:
            pass

if __name__ == "__main__":
    s3 = S3Client(
        endpoint_url='http://localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        verify_ssl=False
    )
    test_lifecycle_transitions(s3)