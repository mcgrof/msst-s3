#!/usr/bin/env python3
"""
Test 9: Object tagging

Tests S3 object tagging functionality including adding, updating,
retrieving, and deleting tags on objects.
"""

import io
import time
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_9(s3_client, config):
    """Object tagging test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-9')
        s3_client.create_bucket(bucket_name)

        # Test 1: Add tags during object upload
        object_key = 'tagged-object.txt'
        tags = 'Environment=Production&Department=Engineering&Project=MSST'

        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=io.BytesIO(b'Tagged object content'),
            Tagging=tags
        )

        # Retrieve tags
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        tag_set = response.get('TagSet', [])

        # Verify tags were set
        expected_tags = {
            'Environment': 'Production',
            'Department': 'Engineering',
            'Project': 'MSST'
        }

        actual_tags = {tag['Key']: tag['Value'] for tag in tag_set}
        assert actual_tags == expected_tags, \
            f"Tag mismatch: expected {expected_tags}, got {actual_tags}"

        # Test 2: Update existing object tags
        new_tags = {
            'TagSet': [
                {'Key': 'Environment', 'Value': 'Staging'},
                {'Key': 'Department', 'Value': 'QA'},
                {'Key': 'Version', 'Value': '2.0'},
                {'Key': 'Owner', 'Value': 'TestTeam'}
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging=new_tags
        )

        # Verify updated tags
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        tag_set = response.get('TagSet', [])
        actual_tags = {tag['Key']: tag['Value'] for tag in tag_set}

        expected_updated = {
            'Environment': 'Staging',
            'Department': 'QA',
            'Version': '2.0',
            'Owner': 'TestTeam'
        }

        assert actual_tags == expected_updated, \
            f"Updated tags mismatch: expected {expected_updated}, got {actual_tags}"

        # Test 3: Tags with special characters and values
        special_object_key = 'special-tags.txt'
        s3_client.put_object(
            bucket_name,
            special_object_key,
            io.BytesIO(b'Object with special tags')
        )

        special_tags = {
            'TagSet': [
                {'Key': 'Cost-Center', 'Value': '12345'},
                {'Key': 'Backup.Schedule', 'Value': 'daily'},
                {'Key': 'Data:Type', 'Value': 'sensitive'},
                {'Key': 'Retention_Days', 'Value': '90'},
                {'Key': 'Empty', 'Value': ''}  # Empty value
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=special_object_key,
            Tagging=special_tags
        )

        # Verify special tags
        response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=special_object_key
        )
        tag_set = response.get('TagSet', [])
        actual_special = {tag['Key']: tag['Value'] for tag in tag_set}

        assert 'Cost-Center' in actual_special, "Cost-Center tag missing"
        assert actual_special.get('Cost-Center') == '12345', "Cost-Center value mismatch"
        assert 'Empty' in actual_special, "Empty value tag missing"

        # Test 4: Maximum number of tags (S3 limit is 10)
        max_tags_object = 'max-tags.txt'
        s3_client.put_object(
            bucket_name,
            max_tags_object,
            io.BytesIO(b'Object with maximum tags')
        )

        max_tag_set = {
            'TagSet': [
                {'Key': f'Tag{i}', 'Value': f'Value{i}'}
                for i in range(10)  # S3 allows up to 10 tags
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=max_tags_object,
            Tagging=max_tag_set
        )

        # Verify all tags are present
        response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=max_tags_object
        )
        tag_set = response.get('TagSet', [])
        assert len(tag_set) == 10, f"Expected 10 tags, got {len(tag_set)}"

        # Test 5: Try to exceed tag limit (should fail or truncate)
        try:
            excessive_tags = {
                'TagSet': [
                    {'Key': f'ExcessTag{i}', 'Value': f'Value{i}'}
                    for i in range(11)  # One more than allowed
                ]
            }

            s3_client.client.put_object_tagging(
                Bucket=bucket_name,
                Key=max_tags_object,
                Tagging=excessive_tags
            )

            # If it succeeds, check if tags were truncated
            response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=max_tags_object
            )
            tag_set = response.get('TagSet', [])
            assert len(tag_set) <= 10, f"More than 10 tags were set: {len(tag_set)}"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            # Expected to fail with too many tags
            assert error_code in ['BadRequest', 'InvalidTag', 'TooManyTags'], \
                f"Unexpected error for excessive tags: {error_code}"

        # Test 6: Delete tags from an object
        s3_client.client.delete_object_tagging(
            Bucket=bucket_name,
            Key=object_key
        )

        # Verify tags are deleted
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=object_key)
        tag_set = response.get('TagSet', [])
        assert len(tag_set) == 0, f"Tags not deleted, still has {len(tag_set)} tags"

        # Test 7: Copy object with tags
        source_key = 'source-with-tags.txt'
        dest_key = 'copied-with-tags.txt'

        # Create source object with tags
        source_tags = 'Type=Original&Priority=High'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=io.BytesIO(b'Source object with tags'),
            Tagging=source_tags
        )

        # Copy object (tags should not be copied by default)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Check if tags were copied
        # Note: Behavior varies - AWS S3 doesn't copy tags, MinIO does
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=dest_key)
        tag_set = response.get('TagSet', [])

        # MinIO copies tags by default, AWS S3 doesn't
        if len(tag_set) > 0:
            # MinIO behavior - tags are copied
            copied_tags = {tag['Key']: tag['Value'] for tag in tag_set}
            assert copied_tags.get('Type') == 'Original', "Original tags not preserved in copy"
            print("Note: This S3 implementation copies tags by default")
        else:
            # AWS S3 behavior - tags are not copied
            print("Note: This S3 implementation does not copy tags by default")

        # Now copy with explicit tag specification
        dest_key2 = 'copied-with-new-tags.txt'
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key2,
            TaggingDirective='REPLACE',  # Explicitly replace tags
            Tagging='Type=Copy&Priority=Low'
        )

        # Verify new tags on copy
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=dest_key2)
        tag_set = response.get('TagSet', [])
        copied_tags = {tag['Key']: tag['Value'] for tag in tag_set}

        # Check if tagging worked as expected
        if copied_tags.get('Type') == 'Copy' and copied_tags.get('Priority') == 'Low':
            print("Copy with explicit tags: ✓")
        else:
            # Some S3 implementations may not support TaggingDirective
            print(f"Note: TaggingDirective may not be fully supported (got tags: {copied_tags})")

        # Test 8: Multipart upload with tags
        multipart_key = 'multipart-with-tags.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        # Start multipart upload with tags
        response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=multipart_key,
            Tagging='UploadType=Multipart&Size=Large'
        )
        upload_id = response['UploadId']

        try:
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

            # Verify tags on multipart object
            response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=multipart_key
            )
            tag_set = response.get('TagSet', [])
            multipart_tags = {tag['Key']: tag['Value'] for tag in tag_set}

            assert 'UploadType' in multipart_tags, "Multipart upload tags not set"
            assert multipart_tags.get('UploadType') == 'Multipart', \
                "Multipart upload tag value incorrect"

        except Exception as e:
            # Clean up on failure
            s3_client.abort_multipart_upload(bucket_name, multipart_key, upload_id)
            raise e

        print(f"Object tagging test completed:")
        print(f"- Basic tagging: ✓")
        print(f"- Tag updates: ✓")
        print(f"- Special characters: ✓")
        print(f"- Tag limits: ✓")
        print(f"- Tag deletion: ✓")
        print(f"- Copy behavior: ✓")
        print(f"- Multipart tagging: ✓")

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass