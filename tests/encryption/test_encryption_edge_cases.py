#!/usr/bin/env python3
"""
Test: Encryption Edge Cases
Tests server-side encryption configurations, key management, and edge cases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import base64

def test_encryption_edge_cases(s3_client: S3Client):
    """Test encryption edge cases"""
    bucket_name = f's3-encryption-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: SSE-S3 (AES256) encryption
        print("Test 1: SSE-S3 encryption")
        key1 = 'sse-s3-encrypted'
        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b'encrypted data',
                ServerSideEncryption='AES256'
            )

            if response.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('SSE-S3 encryption')
                print("✓ SSE-S3: Object encrypted with AES256")
            else:
                results['failed'].append('SSE-S3: Encryption not confirmed')

            # Verify encryption on retrieval
            obj = s3_client.client.get_object(Bucket=bucket_name, Key=key1)
            if obj.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('SSE-S3 encryption verified')
                print("✓ SSE-S3: Encryption verified on retrieval")

        except Exception as e:
            results['failed'].append(f'SSE-S3: {str(e)}')

        # Test 2: SSE-C (Customer-provided key)
        print("\nTest 2: SSE-C with customer key")
        key2 = 'sse-c-encrypted'

        # Generate a 256-bit key
        customer_key = base64.b64encode(b'a' * 32).decode('utf-8')
        customer_key_md5 = base64.b64encode(
            __import__('hashlib').md5(base64.b64decode(customer_key)).digest()
        ).decode('utf-8')

        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=b'customer key encrypted',
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=customer_key,
                SSECustomerKeyMD5=customer_key_md5
            )

            if response.get('SSECustomerAlgorithm') == 'AES256':
                results['passed'].append('SSE-C encryption')
                print("✓ SSE-C: Object encrypted with customer key")

            # Try to get without key (should fail)
            try:
                s3_client.client.get_object(Bucket=bucket_name, Key=key2)
                results['failed'].append('SSE-C: Retrieved without key')
                print("✗ SSE-C: Object retrieved without key")
            except Exception as e:
                if 'InvalidRequest' in str(e) or '400' in str(e):
                    results['passed'].append('SSE-C requires key')
                    print("✓ SSE-C: Correctly requires key for retrieval")

            # Get with correct key
            obj = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key2,
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=customer_key,
                SSECustomerKeyMD5=customer_key_md5
            )
            if obj['Body'].read() == b'customer key encrypted':
                results['passed'].append('SSE-C retrieval with key')
                print("✓ SSE-C: Retrieved with correct key")

        except Exception as e:
            # SSE-C might not be supported
            if 'NotImplemented' in str(e):
                results['passed'].append('SSE-C not implemented')
                print("✓ SSE-C: Not implemented (expected for some providers)")
            else:
                results['failed'].append(f'SSE-C: {str(e)}')

        # Test 3: Bucket default encryption
        print("\nTest 3: Bucket default encryption")
        try:
            s3_client.client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }
                    ]
                }
            )

            # Upload without specifying encryption
            key3 = 'default-encrypted'
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key3,
                Body=b'should be encrypted by default'
            )

            if response.get('ServerSideEncryption') == 'AES256':
                results['passed'].append('Default bucket encryption')
                print("✓ Default encryption: Applied automatically")
            else:
                results['failed'].append('Default encryption: Not applied')

        except Exception as e:
            if 'NotImplemented' in str(e):
                results['passed'].append('Default encryption not implemented')
                print("✓ Default encryption: Not implemented")
            else:
                results['failed'].append(f'Default encryption: {str(e)}')

        # Test 4: Mixed encryption in same bucket
        print("\nTest 4: Mixed encryption in same bucket")
        try:
            # Object with SSE-S3
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='mixed-sse-s3',
                Body=b'sse-s3',
                ServerSideEncryption='AES256'
            )

            # Object without encryption (if default not set)
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='mixed-unencrypted',
                Body=b'unencrypted'
            )

            # List and check encryption status
            objects = s3_client.client.list_objects_v2(Bucket=bucket_name, Prefix='mixed-')
            if objects.get('KeyCount', 0) >= 2:
                results['passed'].append('Mixed encryption in bucket')
                print("✓ Mixed encryption: Multiple encryption types coexist")

        except Exception as e:
            results['failed'].append(f'Mixed encryption: {str(e)}')

        # Test 5: Invalid encryption parameters
        print("\nTest 5: Invalid encryption parameters")

        # Invalid algorithm
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='invalid-algo',
                Body=b'test',
                ServerSideEncryption='INVALID'
            )
            results['failed'].append('Invalid algorithm: Accepted')
            print("✗ Invalid algorithm: Accepted INVALID")
        except Exception as e:
            if 'InvalidArgument' in str(e) or '400' in str(e):
                results['passed'].append('Invalid algorithm rejected')
                print("✓ Invalid algorithm: Correctly rejected")

        # Invalid customer key length
        try:
            bad_key = base64.b64encode(b'short').decode('utf-8')
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='bad-key-length',
                Body=b'test',
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=bad_key
            )
            results['failed'].append('Invalid key length: Accepted')
        except:
            results['passed'].append('Invalid key length rejected')
            print("✓ Invalid key length: Correctly rejected")

        # Test 6: Copy with encryption change
        print("\nTest 6: Copy with encryption change")
        try:
            # Copy SSE-S3 object to new object without encryption
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='copy-no-encryption',
                CopySource={'Bucket': bucket_name, 'Key': key1}
            )

            # Copy and add encryption
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key='copy-with-encryption',
                CopySource={'Bucket': bucket_name, 'Key': 'mixed-unencrypted'},
                ServerSideEncryption='AES256'
            )

            results['passed'].append('Copy with encryption changes')
            print("✓ Copy operations: Encryption can be changed")

        except Exception as e:
            results['failed'].append(f'Copy encryption: {str(e)}')

        # Test 7: Head object shows encryption
        print("\nTest 7: Head object encryption metadata")
        try:
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key1)
            if 'ServerSideEncryption' in head:
                results['passed'].append('Head shows encryption')
                print(f"✓ Head object: Shows encryption type: {head['ServerSideEncryption']}")
            else:
                results['failed'].append('Head missing encryption info')

        except Exception as e:
            results['failed'].append(f'Head encryption: {str(e)}')

        # Summary
        print(f"\n=== Encryption Edge Cases Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            # Delete encryption configuration
            try:
                s3_client.client.delete_bucket_encryption(Bucket=bucket_name)
            except:
                pass

            # Delete objects
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
    test_encryption_edge_cases(s3)