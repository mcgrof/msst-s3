#!/usr/bin/env python3
"""
Script to fix S3Client configurations in all test files
"""

import os
import re

def fix_test_file(filepath):
    """Fix S3Client configuration in a test file"""
    with open(filepath, 'r') as f:
        content = f.read()

    # Pattern to match the old config style
    old_pattern = r'''if __name__ == "__main__":
    config = \{
        's3_endpoint_url': 'http://localhost:9000',
        's3_access_key': 'minioadmin',
        's3_secret_key': 'minioadmin',
        's3_region': 'us-east-1'
    \}
    s3 = S3Client\(config\)'''

    # New pattern to replace with
    new_pattern = '''if __name__ == "__main__":
    s3 = S3Client(
        endpoint_url='http://localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        verify_ssl=False
    )'''

    # Perform replacement
    updated_content = re.sub(old_pattern, new_pattern, content)

    if updated_content != content:
        with open(filepath, 'w') as f:
            f.write(updated_content)
        return True
    return False

def main():
    """Fix all test files"""
    test_dirs = ['tests/edge', 'tests/multipart', 'tests/versioning', 'tests/performance',
                 'tests/acl', 'tests/encryption', 'tests/lifecycle', 'tests/cors',
                 'tests/tagging', 'tests/presigned']

    fixed_count = 0

    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            for filename in os.listdir(test_dir):
                if filename.startswith('test_') and filename.endswith('.py'):
                    filepath = os.path.join(test_dir, filename)
                    if fix_test_file(filepath):
                        print(f"Fixed: {filepath}")
                        fixed_count += 1

    print(f"Total files fixed: {fixed_count}")

if __name__ == "__main__":
    main()