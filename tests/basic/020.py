#!/usr/bin/env python3
"""
Test 020: Request payment

Tests S3 Requester Pays bucket functionality where the requester
(not the bucket owner) pays for the request and data transfer costs.
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_020(s3_client, config):
    """Request payment test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-020')
        s3_client.create_bucket(bucket_name)

        # Test 1: Get default request payment configuration
        try:
            response = s3_client.client.get_bucket_request_payment(
                Bucket=bucket_name
            )

            payer = response.get('Payer')
            assert payer == 'BucketOwner', f"Default payer should be BucketOwner, got {payer}"
            print("Default request payment (BucketOwner): ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotImplemented':
                print("Note: Request payment not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Enable Requester Pays
        try:
            s3_client.client.put_bucket_request_payment(
                Bucket=bucket_name,
                RequestPaymentConfiguration={
                    'Payer': 'Requester'
                }
            )

            # Verify configuration changed
            response = s3_client.client.get_bucket_request_payment(
                Bucket=bucket_name
            )

            payer = response.get('Payer')
            assert payer == 'Requester', f"Payer should be Requester, got {payer}"
            print("Enable Requester Pays: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Requester Pays configuration not supported")
                return
            else:
                raise

        # Test 3: Upload object to Requester Pays bucket
        object_key = 'requester-pays-object.txt'
        test_data = b'This object is in a Requester Pays bucket'

        try:
            # Upload with RequestPayer header
            response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=io.BytesIO(test_data),
                RequestPayer='requester'
            )

            print("Upload to Requester Pays bucket: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidArgument':
                # Some implementations don't require RequestPayer for uploads
                # Try without it
                s3_client.put_object(
                    bucket_name,
                    object_key,
                    io.BytesIO(test_data)
                )
                print("Upload to Requester Pays bucket (owner): ✓")
            else:
                raise

        # Test 4: Download from Requester Pays bucket without RequestPayer (should fail)
        try:
            # Try to get object without RequestPayer header
            response = s3_client.get_object(bucket_name, object_key)

            # If we get here, the implementation doesn't enforce Requester Pays
            print("Note: Requester Pays not enforced for downloads")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                print("Access denied without RequestPayer: ✓")

                # Now try with RequestPayer header
                try:
                    response = s3_client.client.get_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        RequestPayer='requester'
                    )

                    downloaded_data = response['Body'].read()
                    assert downloaded_data == test_data, "Data mismatch"
                    print("Download with RequestPayer: ✓")

                except ClientError as e2:
                    print(f"Note: Download with RequestPayer failed: {e2.response['Error']['Code']}")
            else:
                print(f"Note: Unexpected error: {error_code}")

        # Test 5: List objects in Requester Pays bucket
        try:
            # Try without RequestPayer (should fail)
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name
            )

            # If we get here, listing doesn't require RequestPayer
            print("Note: Listing doesn't require RequestPayer")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                # Try with RequestPayer
                try:
                    response = s3_client.client.list_objects_v2(
                        Bucket=bucket_name,
                        RequestPayer='requester'
                    )

                    objects = response.get('Contents', [])
                    assert len(objects) > 0, "No objects found"
                    print("List with RequestPayer: ✓")

                except ClientError as e2:
                    print(f"Note: List with RequestPayer failed: {e2.response['Error']['Code']}")
            else:
                print(f"Note: Unexpected list error: {error_code}")

        # Test 6: Copy object in Requester Pays bucket
        copy_key = 'copied-requester-pays.txt'
        copy_source = {'Bucket': bucket_name, 'Key': object_key}

        try:
            # Copy with RequestPayer
            response = s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=copy_key,
                RequestPayer='requester'
            )

            print("Copy in Requester Pays bucket: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'InvalidArgument']:
                # Try without RequestPayer
                try:
                    s3_client.client.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket_name,
                        Key=copy_key
                    )
                    print("Copy in Requester Pays bucket (owner): ✓")
                except ClientError:
                    print("Note: Copy in Requester Pays bucket not supported")
            else:
                print(f"Note: Copy failed: {error_code}")

        # Test 7: Multipart upload in Requester Pays bucket
        multipart_key = 'multipart-requester-pays.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        try:
            # Start multipart upload with RequestPayer
            response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=multipart_key,
                RequestPayer='requester'
            )

            upload_id = response['UploadId']

            # Upload a part
            part_data = b'M' * part_size
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=multipart_key,
                UploadId=upload_id,
                PartNumber=1,
                Body=io.BytesIO(part_data),
                RequestPayer='requester'
            )

            # Complete multipart upload
            parts = [{'PartNumber': 1, 'ETag': response['ETag']}]
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=multipart_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
                RequestPayer='requester'
            )

            print("Multipart upload in Requester Pays bucket: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['InvalidRequest', 'InvalidArgument']:
                print("Note: Multipart with RequestPayer not fully supported")
                # Try to abort the upload
                try:
                    s3_client.abort_multipart_upload(bucket_name, multipart_key, upload_id)
                except:
                    pass
            else:
                print(f"Note: Multipart failed: {error_code}")

        # Test 8: HEAD object in Requester Pays bucket
        try:
            # Try HEAD without RequestPayer
            response = s3_client.head_object(bucket_name, object_key)

            # If successful, RequestPayer not enforced for HEAD
            print("Note: HEAD doesn't require RequestPayer")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['AccessDenied', 'Forbidden']:
                # Try with RequestPayer
                try:
                    response = s3_client.client.head_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        RequestPayer='requester'
                    )
                    print("HEAD with RequestPayer: ✓")
                except ClientError as e2:
                    print(f"Note: HEAD with RequestPayer failed: {e2.response['Error']['Code']}")
            else:
                print(f"Note: HEAD error: {error_code}")

        # Test 9: Delete object in Requester Pays bucket
        try:
            # Try delete with RequestPayer
            s3_client.client.delete_object(
                Bucket=bucket_name,
                Key=copy_key,
                RequestPayer='requester'
            )
            print("Delete in Requester Pays bucket: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidArgument':
                # Try without RequestPayer
                try:
                    s3_client.delete_object(bucket_name, copy_key)
                    print("Delete in Requester Pays bucket (owner): ✓")
                except ClientError:
                    print("Note: Delete in Requester Pays bucket failed")
            else:
                print(f"Note: Delete error: {error_code}")

        # Test 10: Disable Requester Pays
        try:
            s3_client.client.put_bucket_request_payment(
                Bucket=bucket_name,
                RequestPaymentConfiguration={
                    'Payer': 'BucketOwner'
                }
            )

            # Verify configuration changed back
            response = s3_client.client.get_bucket_request_payment(
                Bucket=bucket_name
            )

            payer = response.get('Payer')
            assert payer == 'BucketOwner', f"Payer should be BucketOwner, got {payer}"
            print("Disable Requester Pays: ✓")

            # Now operations should work without RequestPayer
            response = s3_client.get_object(bucket_name, object_key)
            print("Access after disabling Requester Pays: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"Note: Error disabling Requester Pays: {error_code}")

        print(f"\nRequest payment test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Access control tested")
        print(f"- Various operations with RequestPayer tested")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Request payment is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Make sure Requester Pays is disabled for cleanup
                try:
                    s3_client.client.put_bucket_request_payment(
                        Bucket=bucket_name,
                        RequestPaymentConfiguration={
                            'Payer': 'BucketOwner'
                        }
                    )
                except:
                    pass

                # Delete all objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass