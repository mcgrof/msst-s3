#!/usr/bin/env python3
"""
Test: CORS Configuration Validation
Tests CORS rules validation, wildcard handling, and method/header restrictions
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import json

def test_cors_validation(s3_client: S3Client):
    """Test CORS configuration validation"""
    bucket_name = f's3-cors-validation-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Valid CORS configuration
        print("Test 1: Valid CORS configuration")
        valid_cors = {
            'CORSRules': [
                {
                    'AllowedOrigins': ['https://example.com', 'https://*.example.com'],
                    'AllowedMethods': ['GET', 'PUT', 'POST', 'DELETE', 'HEAD'],
                    'AllowedHeaders': ['*'],
                    'ExposeHeaders': ['ETag', 'x-amz-request-id'],
                    'MaxAgeSeconds': 3600
                }
            ]
        }

        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=valid_cors
            )

            # Retrieve and verify
            cors = s3_client.client.get_bucket_cors(Bucket=bucket_name)
            if len(cors['CORSRules']) == 1:
                results['passed'].append('Valid CORS configuration')
                print("✓ Valid CORS: Configuration accepted")
            else:
                results['failed'].append('Valid CORS: Configuration altered')

        except Exception as e:
            results['failed'].append(f'Valid CORS: {str(e)}')

        # Test 2: Maximum CORS rules (limit is typically 100)
        print("\nTest 2: Maximum CORS rules")
        max_rules = {'CORSRules': []}
        for i in range(100):
            max_rules['CORSRules'].append({
                'AllowedOrigins': [f'https://site{i}.com'],
                'AllowedMethods': ['GET']
            })

        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=max_rules
            )
            results['passed'].append('100 CORS rules accepted')
            print("✓ Maximum CORS rules: 100 rules accepted")

            # Try to add one more (101st rule)
            max_rules['CORSRules'].append({
                'AllowedOrigins': ['https://site101.com'],
                'AllowedMethods': ['GET']
            })

            try:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name,
                    CORSConfiguration=max_rules
                )
                results['failed'].append('101 CORS rules: Should have been rejected')
                print("✗ 101 CORS rules: Accepted (should reject)")
            except Exception as e:
                if 'TooManyRules' in str(e) or 'InvalidRequest' in str(e):
                    results['passed'].append('CORS rule limit enforced')
                    print("✓ CORS rule limit: Correctly enforced at 100")
                else:
                    results['failed'].append(f'CORS limit: Unexpected error')

        except Exception as e:
            results['failed'].append(f'Max CORS rules: {str(e)}')

        # Test 3: Invalid HTTP methods
        print("\nTest 3: Invalid HTTP methods")
        invalid_methods = {
            'CORSRules': [{
                'AllowedOrigins': ['*'],
                'AllowedMethods': ['GET', 'INVALID', 'PATCH']  # INVALID is not valid
            }]
        }

        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=invalid_methods
            )
            results['failed'].append('Invalid methods: Should have been rejected')
            print("✗ Invalid HTTP methods: Accepted")
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'MalformedXML' in str(e):
                results['passed'].append('Invalid methods rejected')
                print("✓ Invalid HTTP methods: Correctly rejected")
            else:
                results['failed'].append(f'Invalid methods: Unexpected error')

        # Test 4: Wildcard origin combinations
        print("\nTest 4: Wildcard origin combinations")
        wildcard_tests = [
            (['*', 'https://example.com'], 'Wildcard with specific origin'),
            (['https://*.*.example.com'], 'Double wildcard in subdomain'),
            (['*://*.example.com'], 'Wildcard protocol'),
            ([''], 'Empty origin string'),
        ]

        for origins, description in wildcard_tests:
            try:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name,
                    CORSConfiguration={
                        'CORSRules': [{
                            'AllowedOrigins': origins,
                            'AllowedMethods': ['GET']
                        }]
                    }
                )

                # Check if it was accepted or modified
                cors = s3_client.client.get_bucket_cors(Bucket=bucket_name)
                set_origins = cors['CORSRules'][0]['AllowedOrigins']

                if set_origins == origins:
                    results['passed'].append(f'{description} accepted')
                    print(f"✓ {description}: Accepted as-is")
                else:
                    results['passed'].append(f'{description} modified')
                    print(f"✓ {description}: Modified to {set_origins}")

            except Exception as e:
                if 'InvalidArgument' in str(e) or 'MalformedXML' in str(e):
                    results['passed'].append(f'{description} rejected')
                    print(f"✓ {description}: Correctly rejected")
                else:
                    results['failed'].append(f'{description}: {str(e)}')

        # Test 5: MaxAgeSeconds limits
        print("\nTest 5: MaxAgeSeconds limits")
        age_tests = [
            (-1, 'Negative MaxAge'),
            (0, 'Zero MaxAge'),
            (86400, 'One day MaxAge'),
            (31536000, 'One year MaxAge'),
            (999999999, 'Very large MaxAge'),
        ]

        for max_age, description in age_tests:
            try:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name,
                    CORSConfiguration={
                        'CORSRules': [{
                            'AllowedOrigins': ['*'],
                            'AllowedMethods': ['GET'],
                            'MaxAgeSeconds': max_age
                        }]
                    }
                )

                if max_age < 0:
                    results['failed'].append(f'{description}: Accepted negative')
                    print(f"✗ {description}: Accepted (should reject)")
                else:
                    results['passed'].append(f'{description} accepted')
                    print(f"✓ {description}: Accepted")

            except Exception as e:
                if max_age < 0:
                    results['passed'].append(f'{description} rejected')
                    print(f"✓ {description}: Correctly rejected")
                else:
                    results['failed'].append(f'{description}: {str(e)}')

        # Test 6: Delete CORS configuration
        print("\nTest 6: Delete CORS configuration")
        try:
            s3_client.client.delete_bucket_cors(Bucket=bucket_name)

            # Try to get CORS after deletion
            try:
                s3_client.client.get_bucket_cors(Bucket=bucket_name)
                results['failed'].append('Delete CORS: Still exists')
                print("✗ Delete CORS: Configuration still exists")
            except Exception as e:
                if 'NoSuchCORSConfiguration' in str(e) or '404' in str(e):
                    results['passed'].append('CORS deleted successfully')
                    print("✓ Delete CORS: Successfully removed")
                else:
                    results['failed'].append(f'Delete CORS: Unexpected error')

        except Exception as e:
            results['failed'].append(f'Delete CORS: {str(e)}')

        # Summary
        print(f"\n=== CORS Validation Test Results ===")
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
    test_cors_validation(s3)