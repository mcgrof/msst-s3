#!/usr/bin/env python3
"""
Rename test files from 001.py format to 1.py format
"""
import os
import re
import shutil

def rename_test_files(base_dir="/xfs1/mcgrof/msst-s3/tests"):
    """Rename all test files from 001.py to 1.py format"""

    test_dirs = ['basic', 'multipart', 'versioning', 'performance', 'acl',
                 'encryption', 'stress', 'advanced']

    renamed_count = 0

    for dir_name in test_dirs:
        dir_path = os.path.join(base_dir, dir_name)
        if not os.path.exists(dir_path):
            continue

        print(f"Processing {dir_name}/...")

        for filename in os.listdir(dir_path):
            # Match files like 001.py, 002.py, etc
            if re.match(r'^\d{3}\.py$', filename):
                old_path = os.path.join(dir_path, filename)

                # Extract number and remove leading zeros
                num = int(filename[:-3])
                new_filename = f"{num}.py"
                new_path = os.path.join(dir_path, new_filename)

                if old_path != new_path:
                    shutil.move(old_path, new_path)
                    print(f"  Renamed {filename} -> {new_filename}")
                    renamed_count += 1

    print(f"\nTotal files renamed: {renamed_count}")

    # Also need to update imports in run_tests.py and other files
    update_imports(base_dir)

def update_imports(base_dir):
    """Update imports to use new naming"""

    # Update run_tests.py
    run_tests_path = os.path.join(base_dir, '..', 'scripts', 'run_tests.py')
    if os.path.exists(run_tests_path):
        with open(run_tests_path, 'r') as f:
            content = f.read()

        # Update test imports from test_001 to test_1
        updated = re.sub(r'test_(\d{3})', lambda m: f"test_{int(m.group(1))}", content)

        # Update module paths from 001 to 1
        updated = re.sub(r'tests\.(\w+)\.(\d{3})\b',
                        lambda m: f"tests.{m.group(1)}.{int(m.group(2))}", updated)

        if updated != content:
            with open(run_tests_path, 'w') as f:
                f.write(updated)
            print("Updated run_tests.py")

    # Update test function names inside the test files
    test_dirs = ['basic', 'multipart', 'versioning', 'performance', 'acl',
                 'encryption', 'stress', 'advanced']

    for dir_name in test_dirs:
        dir_path = os.path.join(base_dir, dir_name)
        if not os.path.exists(dir_path):
            continue

        for filename in os.listdir(dir_path):
            if filename.endswith('.py') and filename[0].isdigit():
                file_path = os.path.join(dir_path, filename)
                with open(file_path, 'r') as f:
                    content = f.read()

                # Update function names from test_001 to test_1
                num = int(filename[:-3]) if filename[:-3].isdigit() else 0
                if num > 0:
                    old_func = f"def test_{num:03d}"
                    new_func = f"def test_{num}"

                    old_docstring = f"Test {num:03d}:"
                    new_docstring = f"Test {num}:"

                    old_bucket = f"test-{num:03d}"
                    new_bucket = f"test-{num}"

                    updated = content.replace(old_func, new_func)
                    updated = updated.replace(old_docstring, new_docstring)
                    updated = updated.replace(old_bucket, new_bucket)

                    if updated != content:
                        with open(file_path, 'w') as f:
                            f.write(updated)
                        print(f"Updated function names in {dir_name}/{filename}")

if __name__ == "__main__":
    rename_test_files()