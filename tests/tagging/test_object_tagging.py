#!/usr/bin/env python3
"""
Test: Object Tagging Operations
Tests object tagging limits, special characters, and tagging with versioning
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_object_tagging(s3_client: S3Client):
    """Test object tagging operations and limits"""
    bucket_name = f's3-tagging-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic tagging
        print("Test 1: Basic object tagging")
        key1 = 'tagged-object'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key1,
            Body=b'test data',
            Tagging='Environment=Production&Team=Backend&Version=1.0'
        )

        try:
            # Get tags
            response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key1)
            tags = {tag['Key']: tag['Value'] for tag in response['TagSet']}

            if len(tags) == 3 and tags.get('Environment') == 'Production':
                results['passed'].append('Basic tagging')
                print("✓ Basic tagging: Tags set and retrieved")
            else:
                results['failed'].append('Basic tagging: Tags mismatch')

        except Exception as e:
            results['failed'].append(f'Basic tagging: {str(e)}')

        # Test 2: Maximum tags (limit is 10)
        print("\nTest 2: Maximum tags per object")
        key2 = 'max-tags'
        max_tags = '&'.join([f'Tag{i}=Value{i}' for i in range(10)])

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=b'test',
                Tagging=max_tags
            )

            response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key2)
            if len(response['TagSet']) == 10:
                results['passed'].append('Maximum 10 tags')
                print("✓ Maximum tags: 10 tags accepted")
            else:
                results['failed'].append(f"Max tags: Only {len(response['TagSet'])} set")

        except Exception as e:
            results['failed'].append(f'Max tags: {str(e)}')

        # Test 3: Too many tags (should fail)
        print("\nTest 3: Too many tags")
        key3 = 'too-many-tags'
        excess_tags = '&'.join([f'Tag{i}=Value{i}' for i in range(11)])

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key3,
                Body=b'test',
                Tagging=excess_tags
            )
            results['failed'].append('Excess tags: Should have been rejected')
            print("✗ Excess tags: 11 tags accepted (should reject)")
        except Exception as e:
            if 'InvalidTag' in str(e) or 'BadRequest' in str(e):
                results['passed'].append('Excess tags rejected')
                print("✓ Excess tags: Correctly rejected")
            else:
                results['failed'].append(f'Excess tags: Unexpected error')

        # Test 4: Tag key/value limits
        print("\nTest 4: Tag key/value length limits")
        key4 = 'tag-length-limits'

        # Key length limit is 128, value is 256
        long_key = 'k' * 128
        long_value = 'v' * 256

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key4,
                Body=b'test',
                Tagging=f'{long_key}={long_value}'
            )
            results['passed'].append('Max length tags')
            print("✓ Tag length: Maximum lengths accepted")
        except Exception as e:
            results['failed'].append(f'Tag length: {str(e)}')

        # Test too long key
        too_long_key = 'k' * 129
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='too-long-key',
                Body=b'test',
                Tagging=f'{too_long_key}=value'
            )
            results['failed'].append('Too long key: Accepted')
        except:
            results['passed'].append('Too long key rejected')
            print("✓ Too long key: Correctly rejected")

        # Test 5: Special characters in tags
        print("\nTest 5: Special characters in tags")
        key5 = 'special-char-tags'

        special_tags = [
            ('Space-Tag', 'Value with spaces'),
            ('Hyphen-Tag', 'hyphen-value'),
            ('Period.Tag', 'period.value'),
            ('Underscore_Tag', 'underscore_value'),
            ('Number123', '456'),
        ]

        for tag_key, tag_value in special_tags:
            try:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=f'{key5}-{tag_key}',
                    Body=b'test',
                    Tagging=f'{tag_key}={tag_value}'
                )
                results['passed'].append(f'Tag {tag_key}')
                print(f"✓ Special char tag: {tag_key} accepted")
            except Exception as e:
                if 'InvalidTag' in str(e):
                    results['passed'].append(f'Tag {tag_key} restricted')
                    print(f"✓ Special char: {tag_key} appropriately restricted")
                else:
                    results['failed'].append(f'Tag {tag_key}: {str(e)}')

        # Test 6: Update existing tags
        print("\nTest 6: Update existing tags")
        key6 = 'update-tags'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key6,
            Body=b'test',
            Tagging='Original=Value1'
        )

        try:
            # Update tags
            s3_client.client.put_object_tagging(
                Bucket=bucket_name,
                Key=key6,
                Tagging={
                    'TagSet': [
                        {'Key': 'Updated', 'Value': 'Value2'},
                        {'Key': 'New', 'Value': 'Value3'}
                    ]
                }
            )

            # Verify update
            response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key6)
            tags = {tag['Key']: tag['Value'] for tag in response['TagSet']}

            if 'Original' not in tags and 'Updated' in tags:
                results['passed'].append('Tag update replaces all')
                print("✓ Tag update: Replaces all tags")
            else:
                results['failed'].append('Tag update: Original tags remain')

        except Exception as e:
            results['failed'].append(f'Tag update: {str(e)}')

        # Test 7: Delete tags
        print("\nTest 7: Delete all tags")
        try:
            s3_client.client.delete_object_tagging(Bucket=bucket_name, Key=key6)

            response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key6)
            if len(response['TagSet']) == 0:
                results['passed'].append('Delete tags')
                print("✓ Delete tags: All tags removed")
            else:
                results['failed'].append('Delete tags: Some remain')

        except Exception as e:
            results['failed'].append(f'Delete tags: {str(e)}')

        # Test 8: Tagging with versioning
        print("\nTest 8: Tagging with versioning")

        # Enable versioning
        s3_client.client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )

        key8 = 'versioned-tags'

        # Create version 1
        v1 = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key8,
            Body=b'version1',
            Tagging='Version=One'
        )

        # Create version 2
        v2 = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key8,
            Body=b'version2',
            Tagging='Version=Two'
        )

        try:
            # Get tags for each version
            tags_v1 = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key8,
                VersionId=v1['VersionId']
            )
            tags_v2 = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key8,
                VersionId=v2['VersionId']
            )

            v1_value = next((t['Value'] for t in tags_v1['TagSet'] if t['Key'] == 'Version'), None)
            v2_value = next((t['Value'] for t in tags_v2['TagSet'] if t['Key'] == 'Version'), None)

            if v1_value == 'One' and v2_value == 'Two':
                results['passed'].append('Version-specific tags')
                print("✓ Versioned tags: Each version has separate tags")
            else:
                results['failed'].append('Versioned tags: Tags mixed')

        except Exception as e:
            results['failed'].append(f'Versioned tags: {str(e)}')

        # Test 9: Empty tag values
        print("\nTest 9: Empty tag values")
        key9 = 'empty-tag-values'
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key9,
                Body=b'test',
                Tagging='EmptyValue=&AnotherTag=NotEmpty'
            )

            response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key9)
            tags = {tag['Key']: tag['Value'] for tag in response['TagSet']}

            if 'EmptyValue' in tags and tags['EmptyValue'] == '':
                results['passed'].append('Empty tag values')
                print("✓ Empty values: Empty tag values accepted")
            else:
                results['failed'].append('Empty values: Not preserved')

        except Exception as e:
            results['failed'].append(f'Empty values: {str(e)}')

        # Summary
        print(f"\n=== Object Tagging Test Results ===")
        print(f"Passed: {len(results['passed'])}")
        print(f"Failed: {len(results['failed'])}")

        return len(results['failed']) == 0

    finally:
        # Cleanup
        try:
            # Delete all versions if versioning enabled
            versions = s3_client.client.list_object_versions(Bucket=bucket_name)
            if 'Versions' in versions:
                for version in versions['Versions']:
                    s3_client.client.delete_object(
                        Bucket=bucket_name,
                        Key=version['Key'],
                        VersionId=version['VersionId']
                    )

            # Delete remaining objects
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
    test_object_tagging(s3)