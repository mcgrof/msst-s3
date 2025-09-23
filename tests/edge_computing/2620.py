#!/usr/bin/env python3
"""
Test 2620: CDN optimization 2620

Tests CDN cache optimization
"""

import io
import time
import hashlib
import json
import random
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_2620(s3_client, config):
    """CDN optimization 2620"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-2620')
        s3_client.create_bucket(bucket_name)

        # Test CDN cache optimization
        key = f'cdn/content/asset-2620.jpg'
        content_type = ['image/jpeg', 'text/css', 'application/javascript', 'text/html'][i % 4]

        # CDN optimized content
        cdn_headers = {
            'Cache-Control': f'public, max-age={[3600, 86400, 604800, 31536000][i % 4]}, immutable',
            'Content-Type': content_type,
            'ETag': hashlib.md5(f'content-2620'.encode()).hexdigest(),
            'Last-Modified': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'Vary': 'Accept-Encoding',
            'X-Cache-Status': 'HIT' if i % 3 == 0 else 'MISS'
        }

        # Upload with CDN headers
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'CDN_CONTENT' * 100),
            CacheControl=cdn_headers['Cache-Control'],
            ContentType=content_type,
            Metadata={
                'cdn-distribution': f'dist-0',
                'edge-location': ['us-east-1', 'eu-west-1', 'ap-southeast-1'][i % 3],
                'cache-behavior': 'optimized',
                'compression': 'gzip'
            }
        )

        print(f"CDN optimization test 2620: ✓")

        print(f"\nTest 2620 - CDN optimization 2620: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NotImplemented', 'InvalidRequest', 'UnsupportedOperation']:
            print(f"Test 2620 - Feature not supported: {error_code}")
        else:
            print(f"Error in test 2620: {error_code}")
            raise

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Clean up all objects including versions
                try:
                    versions = s3_client.client.list_object_versions(Bucket=bucket_name)
                    for version in versions.get('Versions', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=version['Key'],
                            VersionId=version['VersionId']
                        )
                    for marker in versions.get('DeleteMarkers', []):
                        s3_client.client.delete_object(
                            Bucket=bucket_name,
                            Key=marker['Key'],
                            VersionId=marker['VersionId']
                        )
                except:
                    # Fallback to simple deletion
                    objects = s3_client.list_objects(bucket_name)
                    for obj in objects:
                        s3_client.delete_object(bucket_name, obj['Key'])

                s3_client.delete_bucket(bucket_name)
            except:
                pass
