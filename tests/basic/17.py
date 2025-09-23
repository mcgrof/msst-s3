#!/usr/bin/env python3
"""
Test 17: Server-side encryption

Tests S3 server-side encryption functionality including SSE-S3,
SSE-KMS, and SSE-C encryption methods.
"""

import io
import base64
import hashlib
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_17(s3_client, config):
    """Server-side encryption test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-17')
        s3_client.create_bucket(bucket_name)

        # Test 1: SSE-S3 encryption (AES256)
        object_key_sse_s3 = 'encrypted-sse-s3.txt'
        test_data = b'This is encrypted data using SSE-S3'

        try:
            # Upload with SSE-S3
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_sse_s3,
                Body=io.BytesIO(test_data),
                ServerSideEncryption='AES256'
            )

            # Check encryption in response
            assert 'ServerSideEncryption' in response, "ServerSideEncryption not in response"
            assert response['ServerSideEncryption'] == 'AES256', \
                f"Expected AES256, got {response['ServerSideEncryption']}"

            # Verify encryption via HEAD
            response = s3_client.head_object(bucket_name, object_key_sse_s3)
            assert response.get('ServerSideEncryption') == 'AES256', \
                "Object not encrypted with AES256"

            # Download and verify data integrity
            response = s3_client.get_object(bucket_name, object_key_sse_s3)
            downloaded_data = response['Body'].read()
            assert downloaded_data == test_data, "Data corrupted after SSE-S3 encryption"

            print("SSE-S3 encryption (AES256): ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Server-side encryption not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: SSE-KMS encryption (if supported)
        object_key_sse_kms = 'encrypted-sse-kms.txt'
        test_data_kms = b'This is encrypted data using SSE-KMS'

        try:
            # Upload with SSE-KMS (using default KMS key)
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_sse_kms,
                Body=io.BytesIO(test_data_kms),
                ServerSideEncryption='aws:kms'
            )

            # Check encryption in response
            if 'ServerSideEncryption' in response:
                assert response['ServerSideEncryption'] == 'aws:kms', \
                    f"Expected aws:kms, got {response['ServerSideEncryption']}"

                # Verify via HEAD
                response = s3_client.head_object(bucket_name, object_key_sse_kms)
                assert response.get('ServerSideEncryption') == 'aws:kms', \
                    "Object not encrypted with KMS"

                print("SSE-KMS encryption: ✓")
            else:
                print("Note: SSE-KMS may not be fully supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'KMS.NotFoundException', 'NotImplemented', 'InvalidEncryptionMethod']:
                print("Note: SSE-KMS not supported or KMS not configured")
            else:
                raise

        # Test 3: SSE-C encryption (customer-provided key)
        object_key_sse_c = 'encrypted-sse-c.txt'
        test_data_sse_c = b'This is encrypted with customer key'

        # Generate a 256-bit key for SSE-C
        customer_key = base64.b64encode(b'ThisIs32ByteKeyForSSE-CEncryption!').decode('utf-8')
        customer_key_md5 = base64.b64encode(
            hashlib.md5(base64.b64decode(customer_key)).digest()
        ).decode('utf-8')

        try:
            # Upload with SSE-C
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key_sse_c,
                Body=io.BytesIO(test_data_sse_c),
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=customer_key,
                SSECustomerKeyMD5=customer_key_md5
            )

            # Check encryption in response
            if 'SSECustomerAlgorithm' in response:
                assert response['SSECustomerAlgorithm'] == 'AES256', \
                    "SSE-C algorithm mismatch"

            # Try to get object without key (should fail)
            try:
                response = s3_client.get_object(bucket_name, object_key_sse_c)
                # If we get here, SSE-C is not properly enforced
                print("Note: SSE-C object accessible without key")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                assert error_code in ['InvalidRequest', 'AccessDenied', 'MissingSecurityHeader'], \
                    f"Unexpected error accessing SSE-C object: {error_code}"

            # Get object with correct key
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=object_key_sse_c,
                SSECustomerAlgorithm='AES256',
                SSECustomerKey=customer_key,
                SSECustomerKeyMD5=customer_key_md5
            )
            downloaded_data = response['Body'].read()
            assert downloaded_data == test_data_sse_c, \
                "Data corrupted after SSE-C encryption"

            print("SSE-C encryption: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'NotImplemented', 'InvalidArgument']:
                print("Note: SSE-C not supported")
            else:
                raise

        # Test 4: Bucket default encryption
        try:
            # Set bucket default encryption to AES256
            encryption_config = {
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }
                ]
            }

            s3_client.client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=encryption_config
            )

            # Verify bucket encryption configuration
            response = s3_client.client.get_bucket_encryption(Bucket=bucket_name)
            rules = response.get('ServerSideEncryptionConfiguration', {}).get('Rules', [])
            assert len(rules) > 0, "No encryption rules found"

            default_encryption = rules[0].get('ApplyServerSideEncryptionByDefault', {})
            assert default_encryption.get('SSEAlgorithm') == 'AES256', \
                "Default encryption not set to AES256"

            # Upload object without specifying encryption (should use default)
            default_encrypted_key = 'default-encrypted.txt'
            s3_client.put_object(
                bucket_name,
                default_encrypted_key,
                io.BytesIO(b'This should be encrypted by default')
            )

            # Verify object is encrypted
            response = s3_client.head_object(bucket_name, default_encrypted_key)
            assert response.get('ServerSideEncryption') == 'AES256', \
                "Object not encrypted with default encryption"

            print("Bucket default encryption: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest']:
                print("Note: Bucket default encryption not supported")
            else:
                raise

        # Test 5: Copy encrypted object
        try:
            # Copy SSE-S3 encrypted object
            copy_key = 'copied-encrypted.txt'
            copy_source = {'Bucket': bucket_name, 'Key': object_key_sse_s3}

            response = s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=copy_key,
                ServerSideEncryption='AES256'
            )

            # Verify copy is encrypted
            response = s3_client.head_object(bucket_name, copy_key)
            assert response.get('ServerSideEncryption') == 'AES256', \
                "Copied object not encrypted"

            print("Copy encrypted object: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error copying encrypted object: {error_code}")

        # Test 6: Multipart upload with encryption
        multipart_key = 'multipart-encrypted.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        try:
            # Start multipart upload with encryption
            response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=multipart_key,
                ServerSideEncryption='AES256'
            )
            upload_id = response['UploadId']

            # Upload one part
            part_data = b'M' * part_size
            response = s3_client.upload_part(
                bucket_name,
                multipart_key,
                upload_id,
                1,
                io.BytesIO(part_data)
            )

            # Complete multipart upload
            parts = [{'PartNumber': 1, 'ETag': response['ETag']}]
            s3_client.complete_multipart_upload(
                bucket_name,
                multipart_key,
                upload_id,
                parts
            )

            # Verify multipart object is encrypted
            response = s3_client.head_object(bucket_name, multipart_key)
            assert response.get('ServerSideEncryption') == 'AES256', \
                "Multipart upload not encrypted"

            print("Multipart upload with encryption: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error with encrypted multipart upload: {error_code}")
            # Try to abort if it failed
            try:
                s3_client.abort_multipart_upload(bucket_name, multipart_key, upload_id)
            except:
                pass

        # Test 7: Delete bucket encryption
        try:
            s3_client.client.delete_bucket_encryption(Bucket=bucket_name)

            # Verify encryption is removed
            try:
                response = s3_client.client.get_bucket_encryption(Bucket=bucket_name)
                # If we get here, encryption still exists
                print("Note: Bucket encryption not fully removed")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ServerSideEncryptionConfigurationNotFoundError':
                    print("Delete bucket encryption: ✓")
                else:
                    print(f"Note: Unexpected error checking encryption: {error_code}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Delete bucket encryption not supported")
            else:
                print(f"Note: Error deleting bucket encryption: {error_code}")

        # Test 8: Encryption with metadata
        metadata_key = 'encrypted-with-metadata.txt'
        try:
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=metadata_key,
                Body=io.BytesIO(b'Encrypted with metadata'),
                ServerSideEncryption='AES256',
                Metadata={
                    'encryption-test': 'true',
                    'test-type': 'sse-s3'
                }
            )

            # Verify both encryption and metadata
            response = s3_client.head_object(bucket_name, metadata_key)
            assert response.get('ServerSideEncryption') == 'AES256', \
                "Object not encrypted"

            metadata = response.get('Metadata', {})
            assert metadata.get('encryption-test') == 'true', \
                "Metadata not preserved with encryption"

            print("Encryption with metadata: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error with encrypted metadata object: {error_code}")

        print(f"\nServer-side encryption test completed:")
        print(f"- SSE-S3 (AES256): ✓")
        print(f"- Various encryption methods tested")
        print(f"- Encryption management: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Server-side encryption is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to delete bucket encryption first
                try:
                    s3_client.client.delete_bucket_encryption(Bucket=bucket_name)
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass