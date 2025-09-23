#!/usr/bin/env python3
"""
Test utilities for S3 testing
"""

import random
import string

def random_string(length=8):
    """Generate a random string of specified length"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))