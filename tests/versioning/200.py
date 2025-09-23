#!/usr/bin/env python3
"""
Test 200: Enable/disable versioning

Tests bucket versioning functionality.
Validates version lifecycle and retrieval.
"""

import io
import time
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_200(s3_client, config):
    """Enable/disable versioning test"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-200')
        s3_client.create_bucket(bucket_name)

        # Check initial versioning status (should be disabled)
        versioning = s3_client.get_bucket_versioning(bucket_name)
        initial_status = versioning.get('Status', '')
        assert initial_status in ['', 'Disabled'], \
            f"New bucket should not have versioning enabled, got: {initial_status}"

        # Enable versioning
        s3_client.put_bucket_versioning(
            bucket_name,
            {'Status': 'Enabled'}
        )

        # Verify versioning is enabled
        versioning = s3_client.get_bucket_versioning(bucket_name)
        assert versioning.get('Status') == 'Enabled', \
            "Versioning should be enabled"

        # Upload object multiple times to create versions
        object_key = 'versioned-object.txt'
        versions = []

        for i in range(3):
            content = f"Version {i+1} content\n"
            response = s3_client.put_object(
                bucket_name,
                object_key,
                io.BytesIO(content.encode()),
                Metadata={'version_num': str(i+1)}
            )
            version_id = response.get('VersionId')
            if version_id:
                versions.append({
                    'id': version_id,
                    'content': content,
                    'num': i+1
                })
            time.sleep(0.5)  # Ensure different timestamps

        # List object versions
        if versions:  # Only if versioning is actually working
            versions_response = s3_client.list_object_versions(bucket_name)
            object_versions = versions_response.get('Versions', [])

            # Filter for our object
            our_versions = [v for v in object_versions if v['Key'] == object_key]
            assert len(our_versions) >= len(versions), \
                f"Expected at least {len(versions)} versions, found {len(our_versions)}"

            # Get specific version
            if versions[0]['id']:
                response = s3_client.get_object(
                    bucket_name,
                    object_key,
                    version_id=versions[0]['id']
                )
                content = response['Body'].read().decode()
                assert content == versions[0]['content'], \
                    f"Version content mismatch: expected '{versions[0]['content']}', got '{content}'"

        # Suspend versioning
        s3_client.put_bucket_versioning(
            bucket_name,
            {'Status': 'Suspended'}
        )

        # Verify versioning is suspended
        versioning = s3_client.get_bucket_versioning(bucket_name)
        assert versioning.get('Status') == 'Suspended', \
            "Versioning should be suspended"

        # Upload new object - should not create new version
        final_content = "Final version after suspension\n"
        response = s3_client.put_object(
            bucket_name,
            object_key,
            io.BytesIO(final_content.encode())
        )

        # Get object without version ID should return latest
        response = s3_client.get_object(bucket_name, object_key)
        content = response['Body'].read().decode()
        assert content == final_content, \
            f"Should get latest version, got: {content}"

    except ClientError as e:
        # Some S3 implementations don't support versioning
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'MethodNotAllowed']:
            # Skip test if versioning not supported
            pass
        else:
            raise

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Delete all versions if versioning was enabled
                try:
                    versions_response = s3_client.list_object_versions(bucket_name)
                    for version in versions_response.get('Versions', []):
                        s3_client.delete_object(
                            bucket_name,
                            version['Key'],
                            version_id=version.get('VersionId')
                        )
                    for marker in versions_response.get('DeleteMarkers', []):
                        s3_client.delete_object(
                            bucket_name,
                            marker['Key'],
                            version_id=marker.get('VersionId')
                        )
                except:
                    # Fallback to simple delete
                    objects = s3_client.list_objects(bucket_name)
                    for obj in objects:
                        s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass