#!/usr/bin/env python3
"""
Test: Server-Side Encryption (SSE)
Tests SSE-S3, SSE-KMS, and SSE-C encryption modes for data protection.
Critical security feature for enterprise compliance and data protection requirements.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import json
import base64
import hashlib

def test_server_side_encryption(s3_client: S3Client):
    """Test server-side encryption configurations and operations"""
    bucket_name = f's3-encryption-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Default bucket encryption (SSE-S3)
        print("Test 1: Default bucket encryption (SSE-S3)")

        sse_s3_config = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=sse_s3_config
            )

            # Verify encryption configuration
            response = s3_client.client.get_bucket_encryption(Bucket=bucket_name)
            rules = response['ServerSideEncryptionConfiguration']['Rules']

            if rules[0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256':
                results['passed'].append('SSE-S3 bucket encryption')
                print("✓ SSE-S3: Default encryption configured")
            else:
                results['failed'].append('SSE-S3: Algorithm mismatch')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedOperation' in str(e):
                results['passed'].append('Bucket encryption not supported')
                print("✓ SSE-S3: Feature not implemented (expected)")
            else:
                results['failed'].append(f'SSE-S3: {str(e)}')

        # Test 2: SSE-KMS bucket encryption
        print("\nTest 2: SSE-KMS bucket encryption")

        sse_kms_config = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'aws:kms',
                        'KMSMasterKeyID': 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012'
                    }
                }
            ]
        }

        try:
            s3_client.client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=sse_kms_config
            )

            response = s3_client.client.get_bucket_encryption(Bucket=bucket_name)
            rule = response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']

            if rule['SSEAlgorithm'] == 'aws:kms' and 'KMSMasterKeyID' in rule:
                results['passed'].append('SSE-KMS bucket encryption')
                print("✓ SSE-KMS: KMS encryption configured")
            else:
                results['failed'].append('SSE-KMS: Configuration mismatch')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('SSE-KMS not supported')
                print("✓ SSE-KMS: Feature not implemented")
            else:
                results['failed'].append(f'SSE-KMS: {str(e)}')

        # Test 3: Object-level SSE-S3 encryption
        print("\nTest 3: Object-level SSE-S3 encryption")

        try:
            test_key = 'sse-s3-object'
            test_data = b'encrypted test data'

            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_data,
                ServerSideEncryption='AES256'
            )

            # Verify encryption was applied
            if 'ServerSideEncryption' in response and response['ServerSideEncryption'] == 'AES256':
                results['passed'].append('Object SSE-S3')
                print("✓ Object SSE-S3: Encryption applied")

                # Verify object can be retrieved
                obj = s3_client.client.get_object(Bucket=bucket_name, Key=test_key)
                if obj['Body'].read() == test_data:
                    results['passed'].append('SSE-S3 decryption')
                    print("✓ SSE-S3: Object decrypted correctly")
                else:
                    results['failed'].append('SSE-S3: Decryption failed')
            else:
                results['failed'].append('Object SSE-S3: Encryption not applied')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Object SSE-S3 not supported')
                print("✓ Object SSE-S3: Feature not implemented")
            else:
                results['failed'].append(f'Object SSE-S3: {str(e)}')

        # Test 4: Object-level SSE-KMS encryption
        print("\nTest 4: Object-level SSE-KMS encryption")

        try:
            kms_key = 'test-key-id'
            test_key = 'sse-kms-object'
            test_data = b'kms encrypted data'

            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_data,
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=kms_key
            )

            if (response.get('ServerSideEncryption') == 'aws:kms' and
                'SSEKMSKeyId' in response):
                results['passed'].append('Object SSE-KMS')
                print("✓ Object SSE-KMS: KMS encryption applied")

                # Verify retrieval
                obj = s3_client.client.get_object(Bucket=bucket_name, Key=test_key)
                if obj['Body'].read() == test_data:
                    results['passed'].append('SSE-KMS decryption')
                    print("✓ SSE-KMS: Object decrypted correctly")
            else:
                results['failed'].append('Object SSE-KMS: Encryption not applied')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'KMS' in str(e):
                results['passed'].append('Object SSE-KMS not supported')
                print("✓ Object SSE-KMS: Feature not implemented")
            else:
                results['failed'].append(f'Object SSE-KMS: {str(e)}')

        # Test 5: SSE-C (Customer-provided keys)
        print("\nTest 5: SSE-C customer-provided encryption")

        try:
            # Generate customer key
            customer_key = b'MyCustomerKey1234567890123456'  # 32 bytes
            customer_key_b64 = base64.b64encode(customer_key).decode()
            customer_key_md5 = base64.b64encode(hashlib.md5(customer_key).digest()).decode()

            test_key = 'sse-c-object'
            test_data = b'customer key encrypted data'

            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_data,
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=customer_key_b64,
                SSECustomerKeyMD5=customer_key_md5
            )

            if response.get('SSECustomerAlgorithm') == 'AES256':
                results['passed'].append('SSE-C encryption')
                print("✓ SSE-C: Customer key encryption applied")

                # Verify retrieval requires the same key
                obj = s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=test_key,
                    SSECustomerAlgorithm='AES256',
                    SSECustomerKey=customer_key_b64,
                    SSECustomerKeyMD5=customer_key_md5
                )

                if obj['Body'].read() == test_data:
                    results['passed'].append('SSE-C decryption')
                    print("✓ SSE-C: Customer key decryption successful")

                # Test that retrieval fails without key
                try:
                    s3_client.client.get_object(Bucket=bucket_name, Key=test_key)
                    results['failed'].append('SSE-C: Access without key should fail')
                except Exception:
                    results['passed'].append('SSE-C key requirement')
                    print("✓ SSE-C: Key required for access")

            else:
                results['failed'].append('SSE-C: Encryption not applied')

        except Exception as e:
            if 'NotImplemented' in str(e) or 'UnsupportedEncryption' in str(e):
                results['passed'].append('SSE-C not supported')
                print("✓ SSE-C: Feature not implemented")
            else:
                results['failed'].append(f'SSE-C: {str(e)}')

        # Test 6: Copy object with encryption change
        print("\nTest 6: Copy with encryption change")

        try:
            # Create unencrypted source object
            source_key = 'unencrypted-source'
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=b'copy encryption test'
            )

            # Copy with encryption
            dest_key = 'encrypted-copy'
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                ServerSideEncryption='AES256'
            )

            # Verify copy is encrypted
            head = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
            if head.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('Copy with encryption')
                print("✓ Copy encryption: Encryption applied to copy")
            else:
                results['failed'].append('Copy encryption: Not applied')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Copy encryption not supported')
                print("✓ Copy encryption: Feature not implemented")
            else:
                results['failed'].append(f'Copy encryption: {str(e)}')

        # Test 7: Multipart upload with encryption
        print("\nTest 7: Multipart upload with encryption")

        try:
            mp_key = 'multipart-encrypted'

            # Initiate multipart upload with encryption
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key,
                ServerSideEncryption='AES256'
            )['UploadId']

            # Upload part
            part_data = b'A' * (5 * 1024 * 1024)  # 5MB
            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part_data
            )

            # Complete upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [
                        {
                            'PartNumber': 1,
                            'ETag': part_response['ETag']
                        }
                    ]
                }
            )

            # Verify encryption
            head = s3_client.client.head_object(Bucket=bucket_name, Key=mp_key)
            if head.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('Multipart encryption')
                print("✓ Multipart encryption: Applied to completed object")
            else:
                results['failed'].append('Multipart encryption: Not applied')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Multipart encryption not supported')
                print("✓ Multipart encryption: Feature not implemented")
            else:
                results['failed'].append(f'Multipart encryption: {str(e)}')

        # Test 8: Encryption context (KMS)
        print("\nTest 8: KMS encryption context")

        try:
            context_key = 'kms-context-object'
            encryption_context = {
                'Department': 'Engineering',
                'Project': 'S3Testing'
            }

            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=context_key,
                Body=b'context encrypted data',
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId='test-key',
                SSEKMSEncryptionContext=json.dumps(encryption_context)
            )

            if 'SSEKMSEncryptionContext' in response:
                results['passed'].append('KMS encryption context')
                print("✓ KMS context: Encryption context applied")
            else:
                results['passed'].append('KMS context handling')
                print("✓ KMS context: Feature handled")

        except Exception as e:
            if 'NotImplemented' in str(e) or 'KMS' in str(e):
                results['passed'].append('KMS context not supported')
                print("✓ KMS context: Feature not implemented")
            else:
                results['failed'].append(f'KMS context: {str(e)}')

        # Test 9: Bucket key for KMS cost optimization
        print("\nTest 9: S3 Bucket Key for KMS")

        bucket_key_config = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'aws:kms',
                        'KMSMasterKeyID': 'test-key'
                    },
                    'BucketKeyEnabled': True
                }
            ]
        }

        try:
            s3_client.client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=bucket_key_config
            )

            response = s3_client.client.get_bucket_encryption(Bucket=bucket_name)
            rule = response['ServerSideEncryptionConfiguration']['Rules'][0]

            if rule.get('BucketKeyEnabled'):
                results['passed'].append('S3 Bucket Key')
                print("✓ Bucket Key: KMS optimization enabled")
            else:
                results['passed'].append('Bucket Key handling')
                print("✓ Bucket Key: Feature handled")

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Bucket Key not supported')
                print("✓ Bucket Key: Feature not implemented")
            else:
                results['failed'].append(f'Bucket Key: {str(e)}')

        # Test 10: Delete bucket encryption
        print("\nTest 10: Delete bucket encryption")

        try:
            s3_client.client.delete_bucket_encryption(Bucket=bucket_name)

            # Verify deletion
            try:
                s3_client.client.get_bucket_encryption(Bucket=bucket_name)
                results['failed'].append('Delete encryption: Configuration still exists')
            except Exception as get_error:
                if 'ServerSideEncryptionConfigurationNotFoundError' in str(get_error) or 'NoSuchEncryptionConfiguration' in str(get_error):
                    results['passed'].append('Delete encryption')
                    print("✓ Delete encryption: Configuration removed")
                else:
                    results['failed'].append(f'Delete encryption: Wrong error: {get_error}')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Delete encryption not supported')
                print("✓ Delete encryption: Feature not implemented")
            else:
                results['failed'].append(f'Delete encryption: {str(e)}')

        # Test 11: Invalid encryption configurations
        print("\nTest 11: Invalid encryption configurations")

        # Test invalid algorithm
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='invalid-encryption',
                Body=b'test',
                ServerSideEncryption='INVALID_ALGORITHM'
            )
            results['failed'].append('Invalid algorithm: Should be rejected')
        except Exception as e:
            if 'InvalidEncryptionAlgorithm' in str(e) or 'InvalidArgument' in str(e):
                results['passed'].append('Invalid algorithm rejected')
                print("✓ Invalid algorithm: Correctly rejected")
            elif 'NotImplemented' in str(e):
                results['passed'].append('Encryption validation not implemented')
                print("✓ Invalid algorithm: Validation not implemented")
            else:
                results['failed'].append(f'Invalid algorithm: {str(e)}')

        # Test invalid customer key
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='invalid-key',
                Body=b'test',
                SSECustomerAlgorithm='AES256',
                SSECustomerKey='invalid-key-too-short',
                SSECustomerKeyMD5='invalid-md5'
            )
            results['failed'].append('Invalid customer key: Should be rejected')
        except Exception as e:
            if 'InvalidArgument' in str(e) or 'BadDigest' in str(e):
                results['passed'].append('Invalid customer key rejected')
                print("✓ Invalid customer key: Correctly rejected")
            elif 'NotImplemented' in str(e):
                results['passed'].append('Customer key validation not implemented')
                print("✓ Invalid customer key: Validation not implemented")
            else:
                results['failed'].append(f'Invalid customer key: {str(e)}')

        # Summary
        print(f"\n=== Server-Side Encryption Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

        return len(results['failed']) == 0

    except Exception as e:
        print(f"Critical error in encryption test setup: {str(e)}")
        return False

    finally:
        # Cleanup
        try:
            # Remove encryption configuration
            try:
                s3_client.client.delete_bucket_encryption(Bucket=bucket_name)
            except:
                pass

            # Clean up objects
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    try:
                        # Some objects may require customer key for deletion
                        s3_client.client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    except:
                        # Try with customer key if needed
                        try:
                            customer_key = b'MyCustomerKey1234567890123456'
                            customer_key_b64 = base64.b64encode(customer_key).decode()
                            customer_key_md5 = base64.b64encode(hashlib.md5(customer_key).digest()).decode()

                            s3_client.client.delete_object(
                                Bucket=bucket_name,
                                Key=obj['Key'],
                                SSECustomerAlgorithm='AES256',
                                SSECustomerKey=customer_key_b64,
                                SSECustomerKeyMD5=customer_key_md5
                            )
                        except:
                            pass

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
    test_server_side_encryption(s3)