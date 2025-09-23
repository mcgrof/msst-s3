#!/usr/bin/env python3
"""
Test 23: Accelerated transfer

Tests S3 Transfer Acceleration configuration which enables fast,
easy, and secure transfers over long distances.
"""

import io
import time
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_23(s3_client, config):
    """Accelerated transfer test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-23')
        s3_client.create_bucket(bucket_name)

        # Test 1: Get default acceleration configuration
        try:
            response = s3_client.client.get_bucket_accelerate_configuration(
                Bucket=bucket_name
            )

            # Default should be no acceleration or Suspended
            status = response.get('Status')
            if status:
                assert status == 'Suspended', f"Default status should be Suspended, got {status}"
            print("Default acceleration status: Suspended or not configured")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'NoSuchAccelerateConfiguration']:
                print("Note: Transfer Acceleration not supported by this S3 implementation")
                return
            else:
                raise

        # Test 2: Enable Transfer Acceleration
        try:
            s3_client.client.put_bucket_accelerate_configuration(
                Bucket=bucket_name,
                AccelerateConfiguration={
                    'Status': 'Enabled'
                }
            )

            # Verify configuration
            response = s3_client.client.get_bucket_accelerate_configuration(
                Bucket=bucket_name
            )

            status = response.get('Status')
            assert status == 'Enabled', f"Status should be Enabled, got {status}"
            print("Transfer Acceleration enabled: ✓")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NotImplemented', 'InvalidRequest', 'MalformedXML']:
                print("Note: Transfer Acceleration configuration not supported")
                return
            else:
                raise

        # Test 3: Upload with acceleration endpoint (simulated)
        # Note: Actual acceleration requires special endpoint like:
        # bucketname.s3-accelerate.amazonaws.com
        # We'll test the configuration aspect

        object_key = 'accelerated-upload.txt'
        test_data = b'This would be uploaded via accelerated endpoint' * 100

        # Regular upload (acceleration endpoint would be used in production)
        start_time = time.time()
        s3_client.put_object(
            bucket_name,
            object_key,
            io.BytesIO(test_data)
        )
        upload_time = time.time() - start_time

        print(f"Upload completed in {upload_time:.3f}s (acceleration configured)")

        # Test 4: Multipart upload with acceleration
        multipart_key = 'accelerated-multipart.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        try:
            # Start multipart upload
            response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=multipart_key
            )
            upload_id = response['UploadId']

            # Upload parts
            parts = []
            for part_num in range(1, 3):  # Upload 2 parts
                part_data = (b'A' * part_size) if part_num == 1 else (b'B' * part_size)

                response = s3_client.upload_part(
                    bucket_name,
                    multipart_key,
                    upload_id,
                    part_num,
                    io.BytesIO(part_data)
                )

                parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                bucket_name,
                multipart_key,
                upload_id,
                parts
            )

            print("Multipart upload with acceleration: ✓")

        except ClientError as e:
            # Try to abort on failure
            try:
                s3_client.abort_multipart_upload(bucket_name, multipart_key, upload_id)
            except:
                pass
            raise e

        # Test 5: Copy operations with acceleration
        copy_key = 'accelerated-copy.txt'
        copy_source = {'Bucket': bucket_name, 'Key': object_key}

        start_time = time.time()
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=copy_key
        )
        copy_time = time.time() - start_time

        print(f"Copy completed in {copy_time:.3f}s (acceleration configured)")

        # Test 6: Suspend Transfer Acceleration
        s3_client.client.put_bucket_accelerate_configuration(
            Bucket=bucket_name,
            AccelerateConfiguration={
                'Status': 'Suspended'
            }
        )

        # Verify suspension
        response = s3_client.client.get_bucket_accelerate_configuration(
            Bucket=bucket_name
        )

        status = response.get('Status')
        assert status == 'Suspended', f"Status should be Suspended, got {status}"
        print("Transfer Acceleration suspended: ✓")

        # Test 7: Upload after suspension (should still work via regular endpoint)
        suspended_key = 'post-suspension-upload.txt'
        s3_client.put_object(
            bucket_name,
            suspended_key,
            io.BytesIO(b'Upload after acceleration suspended')
        )

        print("Upload after suspension works: ✓")

        # Test 8: Re-enable acceleration
        s3_client.client.put_bucket_accelerate_configuration(
            Bucket=bucket_name,
            AccelerateConfiguration={
                'Status': 'Enabled'
            }
        )

        response = s3_client.client.get_bucket_accelerate_configuration(
            Bucket=bucket_name
        )

        status = response.get('Status')
        assert status == 'Enabled', "Re-enable failed"
        print("Transfer Acceleration re-enabled: ✓")

        # Test 9: Batch operations with acceleration
        batch_count = 10
        batch_start = time.time()

        for i in range(batch_count):
            key = f'accelerated-batch/file-{i:03d}.txt'
            data = f'Batch file {i} content'.encode()
            s3_client.put_object(bucket_name, key, io.BytesIO(data))

        batch_time = time.time() - batch_start
        print(f"Batch upload ({batch_count} files) in {batch_time:.3f}s")

        # Test 10: Download with acceleration (simulated)
        # Downloads can also benefit from acceleration
        download_start = time.time()
        response = s3_client.get_object(bucket_name, object_key)
        downloaded_data = response['Body'].read()
        download_time = time.time() - download_start

        assert len(downloaded_data) == len(test_data), "Download data size mismatch"
        print(f"Download completed in {download_time:.3f}s")

        # Test 11: Check if acceleration is available for bucket name
        # Bucket names with dots are not compatible with acceleration
        test_bucket_names = [
            'valid-bucket-name',
            'bucket.with.dots',  # Not compatible
            'bucket-in-us-east-1',
            'bucket_with_underscore'  # Not compatible
        ]

        compatible_count = 0
        for test_name in test_bucket_names:
            # Check if name is compatible (no dots, no underscores)
            if '.' not in test_name and '_' not in test_name:
                compatible_count += 1

        print(f"Acceleration-compatible bucket names: {compatible_count}/{len(test_bucket_names)}")

        print(f"\nAccelerated transfer test completed:")
        print(f"- Configuration management: ✓")
        print(f"- Upload/download operations: ✓")
        print(f"- Multipart transfers: ✓")
        print(f"- Status changes: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotImplemented':
            print("Transfer Acceleration is not implemented in this S3 provider")
            # This is acceptable for some S3 implementations
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Try to disable acceleration first
                try:
                    s3_client.client.put_bucket_accelerate_configuration(
                        Bucket=bucket_name,
                        AccelerateConfiguration={
                            'Status': 'Suspended'
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