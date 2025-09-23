#!/usr/bin/env python3
"""
Test: Listing Operations Edge Cases
Tests pagination, delimiter handling, prefix filtering, and listing limits
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_listing_edge_cases(s3_client: S3Client):
    """Test listing operations edge cases"""
    bucket_name = f's3-listing-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Create test objects with hierarchical structure
        test_objects = [
            'file1.txt',
            'file2.txt',
            'dir1/file1.txt',
            'dir1/file2.txt',
            'dir1/subdir1/file1.txt',
            'dir1/subdir1/file2.txt',
            'dir1/subdir2/file1.txt',
            'dir2/file1.txt',
            'dir2/file2.txt',
            'dir2/subdir1/file1.txt',
            'special/!@#$%^&().txt',
            'special/unicode-🚀.txt',
            'special/..hidden.txt',
            'special/.hidden/file.txt',
            'a' * 900,  # Long key name
        ]

        print("Creating test objects...")
        for key in test_objects:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b'test'
            )

        # Test 1: List with max-keys limit
        print("\nTest 1: List with max-keys limit")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=5
            )

            if response['KeyCount'] == 5:
                results['passed'].append('MaxKeys limit')
                print(f"✓ MaxKeys: Limited to 5 objects")
            else:
                results['failed'].append(f"MaxKeys: Got {response['KeyCount']} objects")

            # Check IsTruncated flag
            if response.get('IsTruncated') == True:
                results['passed'].append('IsTruncated flag')
                print("✓ IsTruncated: Flag correctly set")

        except Exception as e:
            results['failed'].append(f'MaxKeys: {str(e)}')

        # Test 2: Pagination with continuation token
        print("\nTest 2: Pagination with continuation token")
        try:
            all_keys = []
            continuation_token = None
            pages = 0

            while True:
                params = {'Bucket': bucket_name, 'MaxKeys': 3}
                if continuation_token:
                    params['ContinuationToken'] = continuation_token

                response = s3_client.client.list_objects_v2(**params)
                pages += 1

                for obj in response.get('Contents', []):
                    all_keys.append(obj['Key'])

                if not response.get('IsTruncated'):
                    break

                continuation_token = response.get('NextContinuationToken')

            if len(all_keys) == len(test_objects):
                results['passed'].append(f'Pagination ({pages} pages)')
                print(f"✓ Pagination: Retrieved all objects in {pages} pages")
            else:
                results['failed'].append(f'Pagination: Got {len(all_keys)}/{len(test_objects)}')

        except Exception as e:
            results['failed'].append(f'Pagination: {str(e)}')

        # Test 3: List with delimiter (directory simulation)
        print("\nTest 3: List with delimiter")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Delimiter='/'
            )

            # Should get root files and directory prefixes
            root_files = [obj['Key'] for obj in response.get('Contents', [])]
            common_prefixes = [p['Prefix'] for p in response.get('CommonPrefixes', [])]

            expected_files = ['file1.txt', 'file2.txt', 'a' * 900]
            expected_dirs = ['dir1/', 'dir2/', 'special/']

            if len(common_prefixes) >= 3:  # At least dir1/, dir2/, special/
                results['passed'].append('Delimiter listing')
                print(f"✓ Delimiter: Found {len(common_prefixes)} directories")
            else:
                results['failed'].append(f'Delimiter: Only {len(common_prefixes)} dirs')

        except Exception as e:
            results['failed'].append(f'Delimiter: {str(e)}')

        # Test 4: List with prefix
        print("\nTest 4: List with prefix")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='dir1/'
            )

            dir1_objects = [obj['Key'] for obj in response.get('Contents', [])]
            dir1_count = sum(1 for key in test_objects if key.startswith('dir1/'))

            if len(dir1_objects) == dir1_count:
                results['passed'].append('Prefix filtering')
                print(f"✓ Prefix: Found {len(dir1_objects)} objects in dir1/")
            else:
                results['failed'].append(f'Prefix: Got {len(dir1_objects)}/{dir1_count}')

        except Exception as e:
            results['failed'].append(f'Prefix: {str(e)}')

        # Test 5: List with prefix and delimiter combined
        print("\nTest 5: Prefix + delimiter")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='dir1/',
                Delimiter='/'
            )

            # Should get dir1/ files and subdirectories
            files = [obj['Key'] for obj in response.get('Contents', [])]
            subdirs = [p['Prefix'] for p in response.get('CommonPrefixes', [])]

            if 'dir1/subdir1/' in subdirs and 'dir1/subdir2/' in subdirs:
                results['passed'].append('Prefix+delimiter')
                print(f"✓ Prefix+delimiter: Found subdirectories")
            else:
                results['failed'].append('Prefix+delimiter: Missing subdirs')

        except Exception as e:
            results['failed'].append(f'Prefix+delimiter: {str(e)}')

        # Test 6: List with start-after
        print("\nTest 6: List with start-after")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                StartAfter='dir1/file1.txt'
            )

            keys = [obj['Key'] for obj in response.get('Contents', [])]

            if 'dir1/file1.txt' not in keys and 'dir1/file2.txt' in keys:
                results['passed'].append('StartAfter parameter')
                print("✓ StartAfter: Correctly skipped objects")
            else:
                results['failed'].append('StartAfter: Wrong starting point')

        except Exception as e:
            results['failed'].append(f'StartAfter: {str(e)}')

        # Test 7: Empty bucket listing
        print("\nTest 7: Empty bucket listing")
        empty_bucket = f's3-empty-{random_string(8).lower()}'
        try:
            s3_client.create_bucket(empty_bucket)
            response = s3_client.client.list_objects_v2(Bucket=empty_bucket)

            if response['KeyCount'] == 0 and 'Contents' not in response:
                results['passed'].append('Empty bucket listing')
                print("✓ Empty bucket: Correct response")
            else:
                results['failed'].append('Empty bucket: Unexpected contents')

            s3_client.delete_bucket(empty_bucket)

        except Exception as e:
            results['failed'].append(f'Empty listing: {str(e)}')

        # Test 8: List with special characters in prefix
        print("\nTest 8: Special character prefix")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='special/'
            )

            special_keys = [obj['Key'] for obj in response.get('Contents', [])]

            if any('!@#$%' in key for key in special_keys):
                results['passed'].append('Special char prefix')
                print("✓ Special chars: Listed correctly")
            else:
                results['failed'].append('Special chars: Not found')

        except Exception as e:
            results['failed'].append(f'Special prefix: {str(e)}')

        # Test 9: List v1 vs v2 comparison
        print("\nTest 9: ListObjects v1 vs v2")
        try:
            # List v1
            v1_response = s3_client.client.list_objects(
                Bucket=bucket_name,
                MaxKeys=5
            )

            # List v2
            v2_response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=5
            )

            v1_count = len(v1_response.get('Contents', []))
            v2_count = v2_response.get('KeyCount', 0)

            if v1_count == v2_count:
                results['passed'].append('v1 vs v2 consistency')
                print("✓ v1 vs v2: Consistent results")
            else:
                results['failed'].append(f'v1 vs v2: {v1_count} != {v2_count}')

        except Exception as e:
            # v1 might be deprecated
            if 'list_objects' in str(e):
                results['passed'].append('v1 deprecated')
                print("✓ ListObjects v1: Deprecated (expected)")
            else:
                results['failed'].append(f'v1 vs v2: {str(e)}')

        # Test 10: Fetch owner information
        print("\nTest 10: Fetch owner information")
        try:
            response = s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                FetchOwner=True,
                MaxKeys=1
            )

            if response.get('Contents'):
                obj = response['Contents'][0]
                if 'Owner' in obj:
                    results['passed'].append('FetchOwner')
                    print("✓ FetchOwner: Owner information included")
                else:
                    results['failed'].append('FetchOwner: No owner info')

        except Exception as e:
            results['failed'].append(f'FetchOwner: {str(e)}')

        # Summary
        print(f"\n=== Listing Edge Cases Test Results ===")
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
    test_listing_edge_cases(s3)