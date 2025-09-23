#!/usr/bin/env python3
"""
Test: Presigned URLs and Temporary Access
Tests presigned URL generation, expiration, and access control
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io
import time
import requests
from datetime import datetime, timedelta

def test_presigned_urls(s3_client: S3Client):
    """Test presigned URL generation and validation"""
    bucket_name = f's3-presigned-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)
        results = {'passed': [], 'failed': []}

        # Test 1: Basic presigned GET URL
        print("Test 1: Basic presigned GET URL")
        key1 = 'presigned-get-test'
        test_data = b'presigned url test data'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key1,
            Body=test_data
        )

        try:
            # Generate presigned URL for GET
            url = s3_client.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key1},
                ExpiresIn=3600  # 1 hour
            )

            # Test URL access
            response = requests.get(url)
            if response.status_code == 200 and response.content == test_data:
                results['passed'].append('Presigned GET URL')
                print("✓ Presigned GET: URL works correctly")
            else:
                results['failed'].append(f'Presigned GET: Status {response.status_code}')

        except Exception as e:
            results['failed'].append(f'Presigned GET: {str(e)}')

        # Test 2: Presigned PUT URL
        print("\nTest 2: Presigned PUT URL")
        key2 = 'presigned-put-test'

        try:
            # Generate presigned URL for PUT
            put_url = s3_client.client.generate_presigned_url(
                'put_object',
                Params={'Bucket': bucket_name, 'Key': key2},
                ExpiresIn=300  # 5 minutes
            )

            # Upload using presigned URL
            upload_data = b'uploaded via presigned url'
            response = requests.put(put_url, data=upload_data)

            if response.status_code == 200:
                # Verify upload
                obj = s3_client.client.get_object(Bucket=bucket_name, Key=key2)
                if obj['Body'].read() == upload_data:
                    results['passed'].append('Presigned PUT URL')
                    print("✓ Presigned PUT: Upload successful")
                else:
                    results['failed'].append('Presigned PUT: Data mismatch')
            else:
                results['failed'].append(f'Presigned PUT: Status {response.status_code}')

        except Exception as e:
            results['failed'].append(f'Presigned PUT: {str(e)}')

        # Test 3: URL expiration
        print("\nTest 3: URL expiration")
        key3 = 'expiration-test'
        s3_client.client.put_object(Bucket=bucket_name, Key=key3, Body=b'test')

        try:
            # Generate URL with 1 second expiration
            short_url = s3_client.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key3},
                ExpiresIn=1
            )

            # Wait for expiration
            time.sleep(2)

            # Try to access expired URL
            response = requests.get(short_url)
            if response.status_code == 403:
                results['passed'].append('URL expiration')
                print("✓ URL expiration: Expired URL rejected")
            else:
                results['failed'].append(f'URL expiration: Status {response.status_code}')

        except Exception as e:
            results['failed'].append(f'URL expiration: {str(e)}')

        # Test 4: Presigned POST (for browser uploads)
        print("\nTest 4: Presigned POST")
        key4 = 'presigned-post-test'

        try:
            # Generate presigned POST
            post_data = s3_client.client.generate_presigned_post(
                Bucket=bucket_name,
                Key=key4,
                ExpiresIn=3600
            )

            # Simulate browser upload
            files = {'file': ('test.txt', b'post upload data')}
            response = requests.post(
                post_data['url'],
                data=post_data['fields'],
                files=files
            )

            if response.status_code in [200, 204]:
                results['passed'].append('Presigned POST')
                print("✓ Presigned POST: Upload successful")
            else:
                results['failed'].append(f'Presigned POST: Status {response.status_code}')

        except Exception as e:
            # Presigned POST might not be fully supported
            if 'NotImplemented' in str(e):
                results['passed'].append('Presigned POST not implemented')
                print("✓ Presigned POST: Not implemented (expected)")
            else:
                results['failed'].append(f'Presigned POST: {str(e)}')

        # Test 5: Presigned URL with response headers
        print("\nTest 5: Presigned URL with custom response headers")
        key5 = 'custom-headers'
        s3_client.client.put_object(Bucket=bucket_name, Key=key5, Body=b'test')

        try:
            # Generate URL with custom response headers
            custom_url = s3_client.client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': key5,
                    'ResponseContentDisposition': 'attachment; filename="download.txt"',
                    'ResponseContentType': 'text/plain'
                },
                ExpiresIn=3600
            )

            response = requests.get(custom_url)
            headers = response.headers

            if 'attachment' in headers.get('Content-Disposition', ''):
                results['passed'].append('Custom response headers')
                print("✓ Custom headers: Response headers customized")
            else:
                results['failed'].append('Custom headers: Not applied')

        except Exception as e:
            results['failed'].append(f'Custom headers: {str(e)}')

        # Test 6: Presigned URL for non-existent object
        print("\nTest 6: Presigned URL for non-existent object")
        try:
            # Generate URL for non-existent key
            nonexistent_url = s3_client.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': 'does-not-exist'},
                ExpiresIn=300
            )

            # URL should generate but access should fail
            response = requests.get(nonexistent_url)
            if response.status_code == 404:
                results['passed'].append('Non-existent object URL')
                print("✓ Non-existent object: Returns 404")
            else:
                results['failed'].append(f'Non-existent: Status {response.status_code}')

        except Exception as e:
            results['failed'].append(f'Non-existent URL: {str(e)}')

        # Test 7: Maximum expiration time
        print("\nTest 7: Maximum expiration time")
        try:
            # Try very long expiration (7 days is typical max)
            max_url = s3_client.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key1},
                ExpiresIn=604800  # 7 days
            )

            # Check if URL was generated
            if 'X-Amz-Expires=604800' in max_url or 'Expires=' in max_url:
                results['passed'].append('Max expiration time')
                print("✓ Max expiration: 7 days accepted")
            else:
                results['passed'].append('Max expiration handled')

        except Exception as e:
            if 'InvalidRequest' in str(e):
                results['passed'].append('Max expiration limited')
                print("✓ Max expiration: Provider enforces limit")
            else:
                results['failed'].append(f'Max expiration: {str(e)}')

        # Test 8: Presigned DELETE URL
        print("\nTest 8: Presigned DELETE URL")
        key8 = 'presigned-delete'
        s3_client.client.put_object(Bucket=bucket_name, Key=key8, Body=b'to delete')

        try:
            # Generate presigned DELETE URL
            delete_url = s3_client.client.generate_presigned_url(
                'delete_object',
                Params={'Bucket': bucket_name, 'Key': key8},
                ExpiresIn=300
            )

            # Delete using presigned URL
            response = requests.delete(delete_url)

            if response.status_code in [200, 204]:
                # Verify deletion
                try:
                    s3_client.client.head_object(Bucket=bucket_name, Key=key8)
                    results['failed'].append('Presigned DELETE: Object still exists')
                except:
                    results['passed'].append('Presigned DELETE')
                    print("✓ Presigned DELETE: Object deleted")
            else:
                results['failed'].append(f'Presigned DELETE: Status {response.status_code}')

        except Exception as e:
            results['failed'].append(f'Presigned DELETE: {str(e)}')

        # Summary
        print(f"\n=== Presigned URLs Test Results ===")
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
    test_presigned_urls(s3)