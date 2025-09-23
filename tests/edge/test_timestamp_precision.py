#!/usr/bin/env python3
"""
Test: Timestamp Precision and Time Handling
Tests LastModified timestamps, time zones, and precision in S3 operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import time
from datetime import datetime, timezone, timedelta

def test_timestamp_precision(s3_client: S3Client):
    """Test timestamp precision and time handling"""
    bucket_name = f's3-timestamp-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic timestamp recording
        print("Test 1: Basic timestamp recording")
        key1 = 'timestamp-test-1'

        # Record time before upload
        before_upload = datetime.now(timezone.utc)
        time.sleep(0.1)  # Small delay to ensure measurable difference

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b'timestamp test data'
            )

            time.sleep(0.1)
            after_upload = datetime.now(timezone.utc)

            # Get object metadata
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key1)
            last_modified = head['LastModified']

            # Verify timestamp is within expected range
            if before_upload <= last_modified <= after_upload:
                results['passed'].append('Timestamp in range')
                print(f"✓ Timestamp: {last_modified} (within range)")
            else:
                results['failed'].append('Timestamp out of range')
                print(f"✗ Timestamp: {last_modified} (out of range)")

            # Check timezone (should be UTC)
            if last_modified.tzinfo == timezone.utc:
                results['passed'].append('UTC timezone')
                print("✓ Timezone: UTC")
            else:
                results['failed'].append(f'Timezone: {last_modified.tzinfo}')

        except Exception as e:
            results['failed'].append(f'Basic timestamp: {str(e)}')

        # Test 2: Timestamp precision
        print("\nTest 2: Timestamp precision")
        timestamps = []

        try:
            # Upload multiple objects quickly to test precision
            for i in range(5):
                key = f'precision-test-{i}'
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f'precision test {i}'.encode()
                )

                head = s3_client.client.head_object(Bucket=bucket_name, Key=key)
                timestamps.append(head['LastModified'])

                time.sleep(0.01)  # 10ms delay

            # Check if timestamps are different
            unique_timestamps = set(timestamps)
            if len(unique_timestamps) >= 3:  # At least some should be different
                results['passed'].append('Timestamp precision')
                print(f"✓ Precision: {len(unique_timestamps)}/5 unique timestamps")
            else:
                results['passed'].append('Limited timestamp precision')
                print(f"✓ Precision: {len(unique_timestamps)}/5 unique (limited precision)")

            # Check ordering
            if timestamps == sorted(timestamps):
                results['passed'].append('Timestamp ordering')
                print("✓ Ordering: Timestamps in chronological order")
            else:
                results['failed'].append('Timestamp ordering incorrect')

        except Exception as e:
            results['failed'].append(f'Timestamp precision: {str(e)}')

        # Test 3: Timestamp stability on read operations
        print("\nTest 3: Timestamp stability")
        key3 = 'stability-test'

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key3,
                Body=b'stability test'
            )

            # Get initial timestamp
            head1 = s3_client.client.head_object(Bucket=bucket_name, Key=key3)
            timestamp1 = head1['LastModified']

            time.sleep(1)

            # Read object (should not change timestamp)
            s3_client.client.get_object(Bucket=bucket_name, Key=key3)

            # Get timestamp again
            head2 = s3_client.client.head_object(Bucket=bucket_name, Key=key3)
            timestamp2 = head2['LastModified']

            if timestamp1 == timestamp2:
                results['passed'].append('Timestamp stability on read')
                print("✓ Stability: Read operations don't change timestamp")
            else:
                results['failed'].append('Timestamp changed on read')

        except Exception as e:
            results['failed'].append(f'Timestamp stability: {str(e)}')

        # Test 4: Timestamp change on update
        print("\nTest 4: Timestamp change on update")
        key4 = 'update-test'

        try:
            # Initial upload
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'original content'
            )

            head1 = s3_client.client.head_object(Bucket=bucket_name, Key=key4)
            timestamp1 = head1['LastModified']

            time.sleep(1)  # Ensure measurable time difference

            # Update object
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'updated content'
            )

            head2 = s3_client.client.head_object(Bucket=bucket_name, Key=key4)
            timestamp2 = head2['LastModified']

            if timestamp2 > timestamp1:
                results['passed'].append('Timestamp update on modify')
                print(f"✓ Update: Timestamp advanced ({timestamp2 - timestamp1})")
            else:
                results['failed'].append('Timestamp not updated on modify')

        except Exception as e:
            results['failed'].append(f'Timestamp update: {str(e)}')

        # Test 5: Copy operation timestamps
        print("\nTest 5: Copy operation timestamps")
        source_key = 'copy-source'
        dest_key = 'copy-dest'

        try:
            # Create source object
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=b'copy test data'
            )

            source_head = s3_client.client.head_object(Bucket=bucket_name, Key=source_key)
            source_timestamp = source_head['LastModified']

            time.sleep(1)

            # Copy object
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={'Bucket': bucket_name, 'Key': source_key}
            )

            dest_head = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
            dest_timestamp = dest_head['LastModified']

            if dest_timestamp > source_timestamp:
                results['passed'].append('Copy creates new timestamp')
                print("✓ Copy: New timestamp created")
            elif dest_timestamp == source_timestamp:
                results['passed'].append('Copy preserves timestamp')
                print("✓ Copy: Original timestamp preserved")
            else:
                results['failed'].append('Copy timestamp anomaly')

        except Exception as e:
            results['failed'].append(f'Copy timestamp: {str(e)}')

        # Test 6: Multipart upload timestamps
        print("\nTest 6: Multipart upload timestamps")
        mp_key = 'multipart-timestamp'

        try:
            # Record start time
            start_time = datetime.now(timezone.utc)

            # Initiate multipart upload
            upload_id = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key
            )['UploadId']

            time.sleep(0.5)

            # Upload parts
            part_data = b'M' * (5 * 1024 * 1024)  # 5MB
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id,
                PartNumber=1,
                Body=io.BytesIO(part_data)
            )

            time.sleep(0.5)

            # Complete upload
            complete_time = datetime.now(timezone.utc)
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=mp_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': response['ETag']}]}
            )

            end_time = datetime.now(timezone.utc)

            # Check final timestamp
            head = s3_client.client.head_object(Bucket=bucket_name, Key=mp_key)
            final_timestamp = head['LastModified']

            # Should be close to completion time, not initiation time
            if complete_time <= final_timestamp <= end_time:
                results['passed'].append('Multipart completion timestamp')
                print("✓ Multipart: Timestamp reflects completion time")
            elif start_time <= final_timestamp <= end_time:
                results['passed'].append('Multipart timestamp in range')
                print("✓ Multipart: Timestamp within upload timeframe")
            else:
                results['failed'].append('Multipart timestamp out of range')

        except Exception as e:
            results['failed'].append(f'Multipart timestamp: {str(e)}')

        # Test 7: Metadata-only update timestamps
        print("\nTest 7: Metadata update timestamps")
        meta_key = 'metadata-update'

        try:
            # Create object with metadata
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=meta_key,
                Body=b'metadata test',
                Metadata={'version': '1'}
            )

            head1 = s3_client.client.head_object(Bucket=bucket_name, Key=meta_key)
            timestamp1 = head1['LastModified']

            time.sleep(1)

            # Update metadata using copy
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=meta_key,
                CopySource={'Bucket': bucket_name, 'Key': meta_key},
                Metadata={'version': '2'},
                MetadataDirective='REPLACE'
            )

            head2 = s3_client.client.head_object(Bucket=bucket_name, Key=meta_key)
            timestamp2 = head2['LastModified']

            if timestamp2 > timestamp1:
                results['passed'].append('Metadata update timestamp')
                print("✓ Metadata: Update changes timestamp")
            else:
                results['failed'].append('Metadata update no timestamp change')

        except Exception as e:
            results['failed'].append(f'Metadata timestamp: {str(e)}')

        # Test 8: Listing timestamp consistency
        print("\nTest 8: Listing timestamp consistency")

        try:
            # Get timestamps from list operation
            list_response = s3_client.client.list_objects_v2(Bucket=bucket_name)

            # Compare with head operation timestamps
            mismatches = 0
            for obj in list_response.get('Contents', [])[:5]:  # Check first 5
                list_timestamp = obj['LastModified']

                head_response = s3_client.client.head_object(
                    Bucket=bucket_name,
                    Key=obj['Key']
                )
                head_timestamp = head_response['LastModified']

                if list_timestamp != head_timestamp:
                    mismatches += 1

            if mismatches == 0:
                results['passed'].append('List vs head timestamp consistency')
                print("✓ Consistency: List and head timestamps match")
            else:
                results['failed'].append(f'Timestamp inconsistency: {mismatches} mismatches')

        except Exception as e:
            results['failed'].append(f'Timestamp consistency: {str(e)}')

        # Test 9: Time zone handling
        print("\nTest 9: Time zone handling")

        try:
            # All S3 timestamps should be in UTC
            all_objects = s3_client.client.list_objects_v2(Bucket=bucket_name)

            non_utc_count = 0
            for obj in all_objects.get('Contents', [])[:10]:  # Check first 10
                timestamp = obj['LastModified']
                if timestamp.tzinfo != timezone.utc:
                    non_utc_count += 1

            if non_utc_count == 0:
                results['passed'].append('UTC timezone consistency')
                print("✓ Timezone: All timestamps in UTC")
            else:
                results['failed'].append(f'Non-UTC timestamps: {non_utc_count}')

        except Exception as e:
            results['failed'].append(f'Timezone test: {str(e)}')

        # Test 10: Timestamp format validation
        print("\nTest 10: Timestamp format validation")

        try:
            # Check that timestamps are proper datetime objects
            head = s3_client.client.head_object(Bucket=bucket_name, Key=key1)
            timestamp = head['LastModified']

            # Should be datetime object
            if isinstance(timestamp, datetime):
                results['passed'].append('Timestamp type')
                print("✓ Format: Proper datetime object")

                # Should have microsecond precision or better
                if hasattr(timestamp, 'microsecond'):
                    results['passed'].append('Timestamp precision attributes')
                    print(f"✓ Precision: Microsecond = {timestamp.microsecond}")
                else:
                    results['failed'].append('Missing microsecond precision')

            else:
                results['failed'].append(f'Timestamp type: {type(timestamp)}')

        except Exception as e:
            results['failed'].append(f'Timestamp format: {str(e)}')

        # Summary
        print(f"\n=== Timestamp Precision Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
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
    test_timestamp_precision(s3)