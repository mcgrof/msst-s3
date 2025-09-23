#!/usr/bin/env python3
"""
Test 001: Unicode and Special Characters Stress Test
Tests S3's handling of various Unicode characters, emoji, and special characters
in object keys, metadata, and tags.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.s3_client import S3Client
from common.test_utils import random_string
import io

def test_unicode_stress(s3_client: S3Client):
    """Test Unicode handling in keys, metadata, and tags"""
    bucket_name = f's3-unicode-test-{random_string(8).lower()}'

    try:
        s3_client.create_bucket(bucket_name)

        # Test cases with various Unicode characters
        test_cases = [
            # Emoji in keys
            ('📁/🗂️/📄.txt', b'emoji path test', 'Emoji in path'),
            ('test-😀-😎-🚀.data', b'emoji in filename', 'Emoji in filename'),

            # Chinese, Japanese, Korean characters
            ('文件夹/文档.txt', b'chinese chars', 'Chinese characters'),
            ('フォルダ/ドキュメント.txt', b'japanese chars', 'Japanese characters'),
            ('폴더/문서.txt', b'korean chars', 'Korean characters'),

            # Arabic and Hebrew (RTL languages)
            ('مجلد/ملف.txt', b'arabic chars', 'Arabic characters'),
            ('תיקייה/קובץ.txt', b'hebrew chars', 'Hebrew characters'),

            # Special Unicode blocks
            ('test\u200B\u200Cfile.txt', b'zero width chars', 'Zero-width characters'),
            ('test\u0301\u0302\u0303.txt', b'combining chars', 'Combining diacriticals'),

            # Mathematical symbols
            ('∑∏∫∂/√∞±≠.txt', b'math symbols', 'Mathematical symbols'),

            # Mixed cases
            ('🚀/文件/フォルダ/مجلد/file.txt', b'mixed unicode', 'Mixed Unicode path'),
        ]

        results = {'passed': [], 'failed': []}

        for key, data, description in test_cases:
            try:
                # Test upload
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=io.BytesIO(data),
                    Metadata={
                        'test-description': description,
                        'unicode-metadata': '测试元数据🎯'
                    }
                )

                # Test retrieval
                response = s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=key
                )
                retrieved = response['Body'].read()

                # Test listing
                list_response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=key[:5] if len(key) > 5 else key
                )

                if retrieved == data:
                    results['passed'].append(description)
                    print(f"✓ {description}: PASSED")
                else:
                    results['failed'].append(f"{description}: Data mismatch")
                    print(f"✗ {description}: Data mismatch")

            except Exception as e:
                results['failed'].append(f"{description}: {str(e)}")
                print(f"✗ {description}: {str(e)}")

        # Test Unicode in tags
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key='tagged-object',
                Body=b'test',
                Tagging='emoji=🏷️&chinese=标签&arabic=علامة'
            )
            results['passed'].append('Unicode in tags')
            print("✓ Unicode in tags: PASSED")
        except Exception as e:
            results['failed'].append(f"Unicode in tags: {str(e)}")
            print(f"✗ Unicode in tags: {str(e)}")

        # Summary
        print(f"\n=== Unicode Stress Test Results ===")
        print(f"Passed: {len(results['passed'])}/{len(test_cases) + 1}")
        print(f"Failed: {len(results['failed'])}/{len(test_cases) + 1}")

        if results['failed']:
            print("\nFailed tests:")
            for failure in results['failed']:
                print(f"  - {failure}")

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
    test_unicode_stress(s3)