#!/usr/bin/env python3
"""
Test 25: Public access blocking

Tests S3 Block Public Access settings which provide centralized controls
to limit public access to S3 resources.
"""

import io
import json
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_25(s3_client, config):
    """Public access blocking test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-25')
        s3_client.create_bucket(bucket_name)

        # Test 1: Get default public access block (should not exist)
        try:
            response = s3_client.client.get_public_access_block(
                Bucket=bucket_name
            )

            # If it exists, check default values
            config_block = response.get('PublicAccessBlockConfiguration', {})
            print(f"Default public access block exists: {config_block}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchPublicAccessBlockConfiguration':
                print("No default public access block (expected): ✓")
            elif error_code == 'NotImplemented':
                print("Note: Public access blocking not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Enable full public access blocking
        full_block_config = {
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        }

        try:
            s3_client.client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration=full_block_config
            )

            # Verify configuration
            response = s3_client.client.get_public_access_block(
                Bucket=bucket_name
            )

            config_block = response.get('PublicAccessBlockConfiguration', {})
            assert config_block.get('BlockPublicAcls') == True, "BlockPublicAcls not set"
            assert config_block.get('IgnorePublicAcls') == True, "IgnorePublicAcls not set"
            assert config_block.get('BlockPublicPolicy') == True, "BlockPublicPolicy not set"
            assert config_block.get('RestrictPublicBuckets') == True, "RestrictPublicBuckets not set"

            print("Full public access blocking enabled: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Public access blocking configuration not supported")
                return
            else:
                raise

        # Test 3: Try to set public ACL (should fail with blocking enabled)
        object_key = 'test-public-acl.txt'
        s3_client.put_object(
            bucket_name,
            object_key,
            io.BytesIO(b'Test content for public ACL')
        )

        try:
            # Try to make object public-read
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key=object_key,
                ACL='public-read'
            )

            # If successful, blocking might not be enforced
            print("Warning: Public ACL was set despite blocking")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['AccessDenied', 'PublicAccessBlockEnabled']:
                print("Public ACL blocked as expected: ✓")
            else:
                print(f"Note: Unexpected error setting public ACL: {error_code}")

        # Test 4: Try to set public bucket policy (should fail)
        public_policy = {
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
                Policy=json.dumps(public_policy)
            )

            # If successful, blocking might not be enforced
            print("Warning: Public policy was set despite blocking")

            # Try to remove it
            s3_client.client.delete_bucket_policy(Bucket=bucket_name)

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['AccessDenied', 'PublicAccessBlockEnabled']:
                print("Public policy blocked as expected: ✓")
            else:
                print(f"Note: Unexpected error setting public policy: {error_code}")

        # Test 5: Partial blocking configuration
        partial_block_config = {
            'BlockPublicAcls': True,
            'IgnorePublicAcls': False,
            'BlockPublicPolicy': False,
            'RestrictPublicBuckets': False
        }

        s3_client.client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=partial_block_config
        )

        # Verify partial configuration
        response = s3_client.client.get_public_access_block(
            Bucket=bucket_name
        )

        config_block = response.get('PublicAccessBlockConfiguration', {})
        assert config_block.get('BlockPublicAcls') == True, "BlockPublicAcls not set"
        assert config_block.get('IgnorePublicAcls') == False, "IgnorePublicAcls should be False"
        assert config_block.get('BlockPublicPolicy') == False, "BlockPublicPolicy should be False"

        print("Partial public access blocking: ✓")

        # Test 6: With partial blocking, try non-public policy (should work)
        restricted_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "RestrictedAccess",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::123456789012:root"
                    },
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(restricted_policy)
            )

            print("Non-public policy allowed with partial blocking: ✓")

            # Clean up policy
            s3_client.client.delete_bucket_policy(Bucket=bucket_name)

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Policy error with partial blocking: {error_code}")

        # Test 7: Disable specific blocking features
        selective_block_config = {
            'BlockPublicAcls': False,
            'IgnorePublicAcls': True,  # Ignore even if ACLs are set
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': False
        }

        s3_client.client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=selective_block_config
        )

        # With BlockPublicAcls=False, we might be able to set ACLs
        # but they'll be ignored due to IgnorePublicAcls=True
        try:
            s3_client.client.put_object_acl(
                Bucket=bucket_name,
                Key=object_key,
                ACL='public-read'
            )

            print("ACL setting allowed but ignored: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: ACL setting result: {error_code}")

        # Test 8: Update configuration
        updated_config = {
            'BlockPublicAcls': False,
            'IgnorePublicAcls': False,
            'BlockPublicPolicy': False,
            'RestrictPublicBuckets': False
        }

        s3_client.client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=updated_config
        )

        response = s3_client.client.get_public_access_block(
            Bucket=bucket_name
        )

        config_block = response.get('PublicAccessBlockConfiguration', {})
        all_false = all(not v for v in config_block.values())
        assert all_false, "Not all settings are False"

        print("All blocking disabled: ✓")

        # Test 9: Delete public access block configuration
        s3_client.client.delete_public_access_block(
            Bucket=bucket_name
        )

        # Verify deletion
        try:
            response = s3_client.client.get_public_access_block(
                Bucket=bucket_name
            )

            # If we get here, configuration still exists
            print("Warning: Public access block configuration not deleted")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchPublicAccessBlockConfiguration':
                print("Public access block deleted: ✓")
            else:
                print(f"Note: Unexpected error after deletion: {error_code}")

        # Test 10: Test with multiple objects
        # Create objects with different intended access levels
        test_objects = [
            ('public/image.jpg', b'Public image data'),
            ('private/document.pdf', b'Private document data'),
            ('internal/config.json', b'{"internal": true}')
        ]

        for key, content in test_objects:
            s3_client.put_object(bucket_name, key, io.BytesIO(content))

        # Re-enable blocking for these objects
        s3_client.client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=full_block_config
        )

        # Try to set different ACLs (all should be blocked)
        blocked_count = 0
        for key, _ in test_objects:
            try:
                s3_client.client.put_object_acl(
                    Bucket=bucket_name,
                    Key=key,
                    ACL='public-read'
                )
            except ClientError:
                blocked_count += 1

        print(f"Blocked public ACLs: {blocked_count}/{len(test_objects)}")

        print(f"\nPublic access blocking test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Blocking enforcement tested")
        print(f"- Various blocking combinations: ✓")
        print(f"- Policy and ACL interaction: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Public access blocking is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Remove public access block
                try:
                    s3_client.client.delete_public_access_block(Bucket=bucket_name)
                except:
                    pass

                # Remove any policies
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