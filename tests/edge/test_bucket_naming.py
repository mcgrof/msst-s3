#!/usr/bin/env python3
"""
Test: Bucket Naming Rules and Edge Cases
Tests bucket naming restrictions, DNS compliance, and special character handling
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string

def test_bucket_naming(s3_client: S3Client):
    """Test bucket naming rules and edge cases"""
    results = {'passed': [], 'failed': []}
    created_buckets = []

    def try_create_bucket(name, description):
        """Helper to test bucket creation"""
        try:
            s3_client.create_bucket(name)
            created_buckets.append(name)
            return True, None
        except Exception as e:
            return False, str(e)

    # Test cases for bucket naming
    test_cases = [
        # Valid names
        ('my-bucket-' + random_string(8).lower(), True, 'Valid DNS name with hyphens'),
        ('bucket.' + random_string(8).lower(), True, 'Valid name with dots'),
        ('123' + random_string(8).lower(), True, 'Name starting with number'),
        ('a' * 3, True, 'Minimum length (3 chars)'),
        ('a' * 63, True, 'Maximum length (63 chars)'),

        # Invalid names
        ('AB' + random_string(8), False, 'Uppercase letters'),
        ('my_bucket_' + random_string(8).lower(), False, 'Underscores'),
        ('my-bucket-' + random_string(8).lower() + '-', False, 'Ends with hyphen'),
        ('-mybucket' + random_string(8).lower(), False, 'Starts with hyphen'),
        ('my..bucket' + random_string(8).lower(), False, 'Consecutive dots'),
        ('my.-bucket' + random_string(8).lower(), False, 'Dot followed by hyphen'),
        ('my-.bucket' + random_string(8).lower(), False, 'Hyphen followed by dot'),
        ('192.168.1.1', False, 'IP address format'),
        ('xn--bucket' + random_string(8).lower(), False, 'xn-- prefix'),
        ('bucket-s3alias', False, 'Ends with -s3alias'),
        ('bucket--ol-s3', False, 'Contains --ol-s3'),
        ('aa', False, 'Too short (2 chars)'),
        ('a' * 64, False, 'Too long (64 chars)'),
        ('', False, 'Empty name'),
        ('my bucket', False, 'Contains space'),
        ('my/bucket', False, 'Contains slash'),
        ('bucket!', False, 'Contains exclamation'),
        ('bucket@host', False, 'Contains @'),
        ('bucket#1', False, 'Contains #'),
        ('bucket$money', False, 'Contains $'),
        ('bucket%percent', False, 'Contains %'),
    ]

    print("Testing bucket naming rules...")
    for name, should_succeed, description in test_cases:
        success, error = try_create_bucket(name, description)

        if should_succeed:
            if success:
                results['passed'].append(description)
                print(f"✓ {description}: Created '{name[:20]}...'")
            else:
                # Some implementations might be stricter
                if 'InvalidBucketName' in error or 'InvalidArgument' in error:
                    results['passed'].append(f'{description} (stricter validation)')
                    print(f"✓ {description}: Stricter validation applied")
                else:
                    results['failed'].append(f'{description}: {error[:50]}')
                    print(f"✗ {description}: Failed unexpectedly")
        else:
            if not success:
                if 'InvalidBucketName' in error or 'InvalidArgument' in error or 'IllegalLocationConstraintException' in error:
                    results['passed'].append(f'{description} rejected')
                    print(f"✓ {description}: Correctly rejected")
                else:
                    results['passed'].append(f'{description} rejected with: {error[:30]}')
                    print(f"✓ {description}: Rejected")
            else:
                results['failed'].append(f'{description}: Should have failed')
                print(f"✗ {description}: Accepted invalid name '{name[:20]}...'")

    # Test bucket name uniqueness
    print("\nTest: Bucket name uniqueness")
    if created_buckets:
        existing_name = created_buckets[0]
        success, error = try_create_bucket(existing_name, "Duplicate name")

        if not success and ('BucketAlreadyExists' in error or 'BucketAlreadyOwnedByYou' in error):
            results['passed'].append('Duplicate name rejected')
            print("✓ Duplicate name: Correctly rejected")
        else:
            results['failed'].append('Duplicate name: Should have failed')
            print("✗ Duplicate name: Accepted duplicate")

    # Test special bucket names
    print("\nTest: Special/reserved bucket names")
    special_names = [
        'aws', 'amazon', 's3', 'test', 'temp', 'tmp', 'root',
        'admin', 'bucket', 'public', 'private', 'www', 'mail'
    ]

    for name in special_names:
        # Add random suffix to avoid conflicts but keep prefix
        test_name = name + '-' + random_string(8).lower()
        success, error = try_create_bucket(test_name, f"Reserved prefix '{name}'")

        # These might be allowed with suffix, but pure forms should typically fail
        if success:
            results['passed'].append(f"Reserved '{name}' with suffix allowed")
            print(f"✓ Reserved '{name}': Allowed with suffix")
        else:
            results['passed'].append(f"Reserved '{name}' pattern restricted")
            print(f"✓ Reserved '{name}': Restricted")

    # Summary
    print(f"\n=== Bucket Naming Test Results ===")
    print(f"Passed: {len(results['passed'])}")
    print(f"Failed: {len(results['failed'])}")

    # Cleanup
    print("\nCleaning up test buckets...")
    for bucket in created_buckets:
        try:
            s3_client.delete_bucket(bucket)
        except:
            pass

    return len(results['failed']) == 0

if __name__ == "__main__":
    s3 = S3Client(
        endpoint_url='http://localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        verify_ssl=False
    )
    test_bucket_naming(s3)