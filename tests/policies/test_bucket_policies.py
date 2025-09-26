#!/usr/bin/env python3
"""
Test: Bucket Policy Management
Tests bucket policy operations including IAM-style JSON policies for access control.
MinIO recommends bucket policies over ACLs for granular access control.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import json
import boto3
from botocore.exceptions import ClientError

def test_bucket_policies(s3_client: S3Client):
    """Test bucket policy operations and IAM-style access control"""
    bucket_name = f's3-policies-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic bucket policy creation
        print("Test 1: Basic bucket policy creation")

        # Define a basic read-only policy for anonymous access
        basic_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(basic_policy)
            )

            # Verify policy was set
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            retrieved_policy = json.loads(response['Policy'])

            if retrieved_policy['Statement'][0]['Action'] == 's3:GetObject':
                results['passed'].append('Basic policy creation')
                print("✓ Basic policy: Created and retrieved successfully")
            else:
                results['failed'].append('Basic policy: Action mismatch')

        except Exception as e:
            results['failed'].append(f'Basic policy: {str(e)}')
            print(f"✗ Basic policy: {str(e)}")

        # Test 2: Policy with multiple statements
        print("\nTest 2: Multi-statement policy")

        multi_statement_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowPublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/public/*"
                },
                {
                    "Sid": "DenyPublicWrite",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["s3:PutObject", "s3:DeleteObject"],
                    "Resource": f"arn:aws:s3:::{bucket_name}/protected/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(multi_statement_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            if len(policy['Statement']) == 2:
                results['passed'].append('Multi-statement policy')
                print("✓ Multi-statement: Policy with 2 statements created")
            else:
                results['failed'].append('Multi-statement: Statement count mismatch')

        except Exception as e:
            results['failed'].append(f'Multi-statement: {str(e)}')

        # Test 3: Condition-based policy
        print("\nTest 3: Conditional access policy")

        conditional_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowGetWithCondition",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/Environment": "Production"
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(conditional_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            if 'Condition' in policy['Statement'][0]:
                results['passed'].append('Conditional policy')
                print("✓ Conditional: Policy with conditions created")
            else:
                results['failed'].append('Conditional: Condition not preserved')

        except Exception as e:
            results['failed'].append(f'Conditional: {str(e)}')

        # Test 4: IP address restriction policy
        print("\nTest 4: IP address restriction policy")

        ip_restricted_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "RestrictByIP",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ],
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": ["192.168.1.0/24", "10.0.0.0/8"]
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(ip_restricted_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            if 'IpAddress' in policy['Statement'][0].get('Condition', {}):
                results['passed'].append('IP restriction policy')
                print("✓ IP restriction: Policy with IP conditions created")
            else:
                results['failed'].append('IP restriction: IP condition not found')

        except Exception as e:
            results['failed'].append(f'IP restriction: {str(e)}')

        # Test 5: Time-based access policy
        print("\nTest 5: Time-based access policy")

        time_based_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "TimeBasedAccess",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Condition": {
                        "DateGreaterThan": {
                            "aws:CurrentTime": "2024-01-01T00:00:00Z"
                        },
                        "DateLessThan": {
                            "aws:CurrentTime": "2030-12-31T23:59:59Z"
                        }
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(time_based_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            conditions = policy['Statement'][0].get('Condition', {})
            if 'DateGreaterThan' in conditions and 'DateLessThan' in conditions:
                results['passed'].append('Time-based policy')
                print("✓ Time-based: Policy with time conditions created")
            else:
                results['failed'].append('Time-based: Time conditions not found')

        except Exception as e:
            results['failed'].append(f'Time-based: {str(e)}')

        # Test 6: Invalid policy rejection
        print("\nTest 6: Invalid policy rejection")

        # Test malformed JSON
        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy='{"invalid": json}'
            )
            results['failed'].append('Invalid JSON: Should have been rejected')
            print("✗ Invalid JSON: Malformed policy accepted")
        except Exception as e:
            if 'MalformedPolicy' in str(e) or 'InvalidPolicy' in str(e) or 'JSONError' in str(e):
                results['passed'].append('Invalid JSON rejected')
                print("✓ Invalid JSON: Correctly rejected")
            else:
                results['failed'].append(f'Invalid JSON: Wrong error: {e}')

        # Test invalid version
        invalid_version_policy = {
            "Version": "2008-10-17",  # Old version
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(invalid_version_policy)
            )
            # Some providers may accept old versions
            results['passed'].append('Old version policy handling')
            print("✓ Old version: Provider accepts 2008-10-17")
        except Exception as e:
            if 'InvalidPolicy' in str(e):
                results['passed'].append('Old version rejected')
                print("✓ Old version: Correctly rejected")
            else:
                results['failed'].append(f'Old version: Unexpected error: {e}')

        # Test 7: Policy size limits
        print("\nTest 7: Policy size limits")

        # Create a large policy (S3 limit is typically 20KB)
        large_statements = []
        for i in range(100):
            large_statements.append({
                "Sid": f"Statement{i}",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/path{i}/*"
            })

        large_policy = {
            "Version": "2012-10-17",
            "Statement": large_statements
        }

        policy_json = json.dumps(large_policy)
        policy_size = len(policy_json)

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=policy_json
            )

            if policy_size > 20000:  # If over 20KB and accepted
                results['passed'].append('Large policy accepted')
                print(f"✓ Large policy: {policy_size} bytes accepted")
            else:
                results['passed'].append('Policy size handling')
                print(f"✓ Policy size: {policy_size} bytes handled")

        except Exception as e:
            if 'PolicyTooLarge' in str(e) or 'EntityTooLarge' in str(e):
                results['passed'].append('Policy size limit enforced')
                print(f"✓ Size limit: {policy_size} bytes correctly rejected")
            else:
                results['failed'].append(f'Large policy: {str(e)}')

        # Test 8: Delete bucket policy
        print("\nTest 8: Delete bucket policy")

        try:
            s3_client.client.delete_bucket_policy(Bucket=bucket_name)

            # Verify deletion
            try:
                s3_client.client.get_bucket_policy(Bucket=bucket_name)
                results['failed'].append('Policy deletion: Policy still exists')
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                    results['passed'].append('Policy deletion')
                    print("✓ Policy deletion: Policy successfully removed")
                else:
                    results['failed'].append(f'Policy deletion: Wrong error: {e}')

        except Exception as e:
            results['failed'].append(f'Policy deletion: {str(e)}')

        # Test 9: Cross-account access policy
        print("\nTest 9: Cross-account access policy")

        cross_account_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "CrossAccountAccess",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::123456789012:root"
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(cross_account_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            principal = policy['Statement'][0].get('Principal', {})
            if 'AWS' in principal:
                results['passed'].append('Cross-account policy')
                print("✓ Cross-account: Policy with AWS principal created")
            else:
                results['failed'].append('Cross-account: AWS principal not found')

        except Exception as e:
            results['failed'].append(f'Cross-account: {str(e)}')

        # Test 10: Resource wildcard patterns
        print("\nTest 10: Resource wildcard patterns")

        wildcard_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "WildcardResources",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}/public/*",
                        f"arn:aws:s3:::{bucket_name}/shared/*/read-only/*"
                    ]
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(wildcard_policy)
            )

            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            policy = json.loads(response['Policy'])

            resources = policy['Statement'][0]['Resource']
            if isinstance(resources, list) and len(resources) == 2:
                results['passed'].append('Wildcard resources')
                print("✓ Wildcard: Multiple resource patterns accepted")
            else:
                results['failed'].append('Wildcard: Resource patterns not preserved')

        except Exception as e:
            results['failed'].append(f'Wildcard: {str(e)}')

        # Summary
        print(f"\n=== Bucket Policy Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    except Exception as e:
        print(f"Critical error in bucket policy test setup: {str(e)}")
        return False

    finally:
        # Cleanup
        try:
            # Remove bucket policy first
            try:
                s3_client.client.delete_bucket_policy(Bucket=bucket_name)
            except:
                pass

            # Clean up any objects
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3_client.client.delete_object(Bucket=bucket_name, Key=obj['Key'])

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
    test_bucket_policies(s3)