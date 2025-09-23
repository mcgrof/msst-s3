#!/usr/bin/env python3
import os
import shutil

dir_path = "/xfs1/mcgrof/msst-s3/tests/multipart"
for filename in os.listdir(dir_path):
    if filename.endswith('.py') and filename[0].isdigit():
        num = int(filename[:-3])
        new_name = f"{num}.py"
        old_path = os.path.join(dir_path, filename)
        new_path = os.path.join(dir_path, new_name)
        if old_path != new_path:
            shutil.move(old_path, new_path)
            print(f"Renamed {filename} -> {new_name}")

            # Update function names inside
            with open(new_path, 'r') as f:
                content = f.read()
            content = content.replace(f"def test_{num:03d}", f"def test_{num}")
            content = content.replace(f"Test {num:03d}:", f"Test {num}:")
            content = content.replace(f"test-{num:03d}", f"test-{num}")
            with open(new_path, 'w') as f:
                f.write(content)
            print(f"  Updated function names")