#!/usr/bin/env python3
"""
Generate multiple test files to reach 1000 tests efficiently.
Each test will be focused and test specific S3 functionality.
"""

import os

# Template for basic object operation tests (031-099)
OBJECT_TEST_TEMPLATE = '''#!/usr/bin/env python3
"""
Test {num}: {description}

{details}
"""

import io
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_{num}(s3_client, config):
    """{short_desc}"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Create test bucket
        bucket_name = fixture.generate_bucket_name('test-{num}')
        s3_client.create_bucket(bucket_name)

        # Test operations
        {test_code}

        print(f"\\n{test_name} completed: ✓")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Error: {{error_code}}")
        raise

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
'''

def generate_object_tests():
    """Generate tests 031-099: Various object operations"""
    tests = []

    # Define test scenarios
    scenarios = [
        (31, "Object metadata operations", "Tests setting and retrieving custom object metadata",
         """# Set custom metadata
        key = 'metadata-test.txt'
        metadata = {
            'custom-key1': 'value1',
            'custom-key2': 'value2',
            'test-id': '031'
        }
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(b'Test content'),
            Metadata=metadata
        )

        # Retrieve and verify metadata
        response = s3_client.head_object(bucket_name, key)
        retrieved_metadata = response.get('Metadata', {})
        assert 'custom-key1' in retrieved_metadata, "Metadata not found"
        print("Custom metadata operations: ✓")"""),

        (32, "Content-Type variations", "Tests various content types for objects",
         """# Test various content types
        content_types = [
            ('file.html', 'text/html', b'<html></html>'),
            ('file.css', 'text/css', b'body { }'),
            ('file.js', 'application/javascript', b'console.log()'),
            ('file.json', 'application/json', b'{"key": "value"}'),
            ('file.xml', 'application/xml', b'<root></root>')
        ]

        for key, content_type, content in content_types:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(content),
                ContentType=content_type
            )

        print(f"Created {len(content_types)} objects with different content types: ✓")"""),

        (33, "Cache-Control headers", "Tests cache control settings for objects",
         """# Test cache control headers
        cache_configs = [
            ('no-cache.txt', 'no-cache'),
            ('public.txt', 'public, max-age=3600'),
            ('private.txt', 'private, max-age=0'),
            ('immutable.txt', 'public, max-age=31536000, immutable')
        ]

        for key, cache_control in cache_configs:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'Cached content'),
                CacheControl=cache_control
            )

            # Verify cache control
            response = s3_client.head_object(bucket_name, key)
            assert response.get('CacheControl') == cache_control

        print(f"Cache-Control headers tested: ✓")"""),

        (34, "Content-Encoding tests", "Tests content encoding like gzip",
         """# Test content encoding
        import gzip

        # Create gzipped content
        original = b'This is the original content that will be compressed' * 10
        compressed = gzip.compress(original)

        key = 'compressed.txt.gz'
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=io.BytesIO(compressed),
            ContentEncoding='gzip',
            ContentType='text/plain'
        )

        # Verify encoding
        response = s3_client.head_object(bucket_name, key)
        assert response.get('ContentEncoding') == 'gzip'
        print("Content-Encoding tested: ✓")"""),

        (35, "Content-Disposition tests", "Tests content disposition for downloads",
         """# Test content disposition
        dispositions = [
            ('inline-file.pdf', 'inline'),
            ('download.pdf', 'attachment'),
            ('named.pdf', 'attachment; filename="custom-name.pdf"')
        ]

        for key, disposition in dispositions:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=io.BytesIO(b'PDF content'),
                ContentDisposition=disposition
            )

            response = s3_client.head_object(bucket_name, key)
            assert response.get('ContentDisposition') == disposition

        print("Content-Disposition tested: ✓")"""),
    ]

    for num, title, details, code in scenarios:
        test_content = OBJECT_TEST_TEMPLATE.format(
            num=num,
            description=title,
            details=details,
            short_desc=title.split('.')[0],
            test_code=code,
            test_name=title
        )
        tests.append((num, test_content))

    return tests

def generate_multipart_tests():
    """Generate tests 100-199: Multipart upload scenarios"""
    tests = []

    # Template for multipart tests
    template = '''#!/usr/bin/env python3
"""
Test {num:03d}: Multipart upload - {scenario}

Tests multipart upload functionality: {details}
"""

import io
import os
from common.fixtures import TestFixture
from botocore.exceptions import ClientError

def test_{num}(s3_client, config):
    """Multipart upload - {scenario}"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        bucket_name = fixture.generate_bucket_name('test-{num}')
        s3_client.create_bucket(bucket_name)

        {test_code}

        print(f"\\nMultipart test {num} completed: ✓")

    finally:
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                s3_client.delete_bucket(bucket_name)
            except:
                pass
'''

    scenarios = [
        (100, "basic 2-part upload", "uploading file in 2 parts",
         """# Basic 2-part multipart upload
        key = 'multipart-2parts.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        parts = []
        for i in range(1, 3):
            data = (b'A' if i == 1 else b'B') * part_size
            response = s3_client.upload_part(bucket_name, key, upload_id, i, io.BytesIO(data))
            parts.append({'PartNumber': i, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("2-part multipart upload: ✓")"""),

        (101, "10-part upload", "uploading file in 10 parts",
         """# 10-part multipart upload
        key = 'multipart-10parts.bin'
        part_size = 5 * 1024 * 1024  # 5MB

        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        parts = []
        for i in range(1, 11):
            data = bytes([65 + i]) * part_size  # Different byte for each part
            response = s3_client.upload_part(bucket_name, key, upload_id, i, io.BytesIO(data))
            parts.append({'PartNumber': i, 'ETag': response['ETag']})

        s3_client.complete_multipart_upload(bucket_name, key, upload_id, parts)
        print("10-part multipart upload: ✓")"""),
    ]

    for num, scenario, details, code in scenarios:
        test_content = template.format(
            num=num,
            scenario=scenario,
            details=details,
            test_code=code
        )
        tests.append((num, test_content))

    return tests

def write_test_files(tests, base_dir="/xfs1/mcgrof/msst-s3/tests"):
    """Write test files to disk"""
    for num, content in tests:
        # Determine directory based on test number
        if num < 100:
            dir_name = "basic"
        elif num < 200:
            dir_name = "multipart"
        elif num < 300:
            dir_name = "versioning"
        elif num < 400:
            dir_name = "acl"
        elif num < 500:
            dir_name = "encryption"
        elif num < 600:
            dir_name = "performance"
        elif num < 700:
            dir_name = "stress"
        else:
            dir_name = "advanced"

        dir_path = os.path.join(base_dir, dir_name)
        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, f"{num}.py")
        with open(file_path, 'w') as f:
            f.write(content)

        print(f"Created test {num}")

if __name__ == "__main__":
    # Generate object tests
    object_tests = generate_object_tests()

    # Generate multipart tests
    multipart_tests = generate_multipart_tests()

    # Combine all tests
    all_tests = object_tests + multipart_tests

    # Write to disk
    write_test_files(all_tests)

    print(f"\nGenerated {len(all_tests)} test files")