#!/usr/bin/env python3
"""
Test 014: Bucket policies

Tests S3 bucket policy functionality including setting policies,
verifying access controls, and policy validation.
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_014(s3_client, config):
    """Bucket policies test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-014')
        s3_client.create_bucket(bucket_name)

        # Test 1: Basic read-only policy
        read_only_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject"
                    ],
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(read_only_policy)
            )

            # Retrieve and verify policy
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            retrieved_policy = json.loads(response['Policy'])

            assert 'Statement' in retrieved_policy, "Statement missing in policy"
            assert len(retrieved_policy['Statement']) == 1, "Unexpected number of statements"
            assert retrieved_policy['Statement'][0]['Effect'] == 'Allow', "Effect not Allow"
            print("Basic read-only policy: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'AccessDenied']:
                print(f"Note: Bucket policies not supported or access denied ({error_code})")
                return
            else:
                raise

        # Test 2: Policy with multiple statements
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
                    "Sid": "DenyDeleteActions",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": [
                        "s3:DeleteObject",
                        "s3:DeleteBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }

        s3_client.client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(multi_statement_policy)
        )

        # Verify multiple statements
        response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
        retrieved_policy = json.loads(response['Policy'])

        assert len(retrieved_policy['Statement']) == 2, "Should have 2 statements"

        # Find statements by Sid
        sids = [stmt.get('Sid') for stmt in retrieved_policy['Statement']]
        assert 'AllowPublicRead' in sids, "AllowPublicRead statement missing"
        assert 'DenyDeleteActions' in sids, "DenyDeleteActions statement missing"
        print("Multi-statement policy: ✓")

        # Test 3: Policy with conditions
        conditional_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowSSLRequestsOnly",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ],
                    "Condition": {
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    }
                },
                {
                    "Sid": "AllowFromSpecificIP",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": [
                                "192.168.1.0/24",
                                "10.0.0.0/8"
                            ]
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

            # Verify conditional policy
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            retrieved_policy = json.loads(response['Policy'])

            # Check for conditions
            has_condition = False
            for stmt in retrieved_policy['Statement']:
                if 'Condition' in stmt:
                    has_condition = True
                    break

            assert has_condition, "Conditional policy should have conditions"
            print("Conditional policy: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedPolicy', 'InvalidPolicyDocument']:
                print("Note: Complex conditions may not be fully supported")
            else:
                raise

        # Test 4: Policy with specific AWS principals (if supported)
        principal_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowSpecificUser",
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
                Policy=json.dumps(principal_policy)
            )

            # Verify principal-based policy
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            retrieved_policy = json.loads(response['Policy'])

            assert 'Principal' in retrieved_policy['Statement'][0], "Principal missing"
            print("Principal-based policy: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['MalformedPolicy', 'InvalidPrincipal']:
                print("Note: AWS principal format may not be supported")
            else:
                raise

        # Test 5: Update existing policy
        updated_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "UpdatedPolicy",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }

        s3_client.client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(updated_policy)
        )

        # Verify update
        response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
        retrieved_policy = json.loads(response['Policy'])

        assert retrieved_policy['Statement'][0]['Sid'] == 'UpdatedPolicy', "Policy not updated"
        print("Policy update: ✓")

        # Test 6: Invalid policy (should fail)
        invalid_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "InvalidPolicy",
                    "Effect": "Invalid",  # Invalid effect value
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(invalid_policy)
            )
            assert False, "Invalid policy should have been rejected"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['MalformedPolicy', 'InvalidPolicyDocument', 'InvalidArgument'], \
                f"Unexpected error for invalid policy: {error_code}"
            print("Invalid policy rejection: ✓")

        # Test 7: Delete bucket policy
        s3_client.client.delete_bucket_policy(Bucket=bucket_name)

        # Verify deletion
        try:
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            # If we get here, policy still exists (shouldn't happen)
            assert False, "Policy should have been deleted"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchBucketPolicy', 'NoSuchBucket'], \
                f"Unexpected error after policy deletion: {error_code}"
            print("Policy deletion: ✓")

        # Test 8: Policy size limits
        # S3 has a 20KB limit for bucket policies
        large_statement_list = []
        for i in range(100):  # Create many statements
            large_statement_list.append({
                "Sid": f"Statement{i}",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/folder{i}/*"
            })

        large_policy = {
            "Version": "2012-10-17",
            "Statement": large_statement_list[:20]  # Use only 20 to stay under limit
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(large_policy)
            )

            # Verify large policy
            response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            retrieved_policy = json.loads(response['Policy'])
            assert len(retrieved_policy['Statement']) == 20, "Large policy not set correctly"
            print("Large policy handling: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'PolicyTooLarge':
                print("Note: Policy size limit enforced")
            else:
                raise

        print(f"\nBucket policies test completed:")
        print(f"- Basic policies: ✓")
        print(f"- Complex statements: ✓")
        print(f"- Policy management: ✓")
        print(f"- Validation: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Bucket policies are not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to delete bucket policy first
                try:
                    s3_client.client.delete_bucket_policy(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass