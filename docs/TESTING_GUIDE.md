# MSST-S3 Testing Guide

Comprehensive guide for testing S3 implementations using the MSST-S3
framework.

## Table of Contents

1. [Test Organization](#test-organization)
2. [Running Tests](#running-tests)
3. [Writing New Tests](#writing-new-tests)
4. [Production Validation](#production-validation)
5. [Performance Testing](#performance-testing)
6. [Debugging Failed Tests](#debugging-failed-tests)
7. [CI/CD Integration](#cicd-integration)

## Test Organization

### Test Numbering Convention

Tests are organized by category with specific number ranges:

| Range | Category | Focus Area |
|-------|----------|------------|
| 001-099 | Basic | Core S3 operations |
| 100-199 | Multipart | Multipart upload operations |
| 200-299 | Versioning | Object versioning |
| 300-399 | ACL | Access control and permissions |
| 400-499 | Encryption | Server-side encryption |
| 500-599 | Lifecycle | Lifecycle management |
| 600-699 | Performance | Benchmarks and throughput |
| 700-799 | Stress | Load and stress testing |
| 800-899 | Compatibility | S3 API compliance |

### Test File Structure

```
tests/
├── basic/
│   ├── 001.py - Create/delete bucket
│   ├── 002.py - Put/get object
│   ├── 003.py - Delete object
│   ├── 004.py - MD5/ETag validation
│   ├── 005.py - Large file integrity
│   ├── 006.py - Concurrent uploads
│   ├── 011.py - Timeout handling
│   └── 012.py - Retry logic
├── multipart/
│   ├── 100.py - Basic multipart
│   ├── 101.py - Parallel parts
│   └── 102.py - Abort upload
├── versioning/
│   └── 200.py - Enable/disable
├── performance/
│   ├── 600.py - Sequential I/O
│   └── 601.py - Concurrent ops
└── common/
    ├── fixtures.py - Test utilities
    ├── s3_client.py - S3 wrapper
    └── validators.py - Validation helpers
```

## Running Tests

### Basic Test Execution

```bash
# Run all tests
make test

# Run specific test by ID
make test TEST=004

# Run test group
make test GROUP=basic
make test GROUP=multipart
make test GROUP=performance

# Verbose output
make test V=1
```

### Test Runner Options

```bash
# Direct test runner usage
python scripts/test-runner.py \
  --config s3_config.yaml \
  --test 004 \
  --output-dir results \
  --verbose

# Run multiple specific tests
python scripts/test-runner.py \
  --config s3_config.yaml \
  --test 004 005 006

# Run with custom timeout
python scripts/test-runner.py \
  --config s3_config.yaml \
  --test 600 \
  --timeout 300
```

### Production Validation

### Quick Start Validation Strategies

Choose the appropriate validation level based on your needs:

| Strategy | Time | Use Case | Command |
|----------|------|----------|---------|
| **Smoke Test** | 2-5 min | Quick functionality check | `make test TEST="001 002 003"` |
| **Critical Path** | 5-10 min | Pre-deployment validation | `python scripts/production-validation.py --quick` |
| **Feature Test** | 15-30 min | Specific feature validation | `make test GROUP=multipart` |
| **Full Production** | 30-60 min | Complete validation | `python scripts/production-validation.py` |

### Validation Commands

```bash
# Quick validation (critical tests only) - 5-10 minutes
python scripts/production-validation.py \
  --config s3_config.yaml \
  --quick

# Full validation suite - 30-60 minutes
python scripts/production-validation.py \
  --config s3_config.yaml \
  --output-dir prod-validation

# Validation with custom criteria
python scripts/production-validation.py \
  --config s3_config.yaml \
  --latency-requirement 500 \
  --throughput-requirement 20

# Progressive validation (escalates on failure)
./scripts/progressive-validation.sh
```

### Validation Success with MinIO

All tests pass successfully with MinIO Docker container:
```
✓ Critical Data Integrity: 100.0% (3/3 passed)
✓ Error Handling & Recovery: 100.0% (2/2 passed)
✓ Multipart Operations: 100.0% (3/3 passed)
✓ Versioning Support: 100.0% (1/1 passed)
✓ Performance Benchmarks: 100.0% (2/2 passed)
================================================================================
✓ PRODUCTION READY - All requirements met
```

For detailed validation strategies, see [VALIDATION_STRATEGIES.md](VALIDATION_STRATEGIES.md)

## Writing New Tests

### Test Template

```python
#!/usr/bin/env python3
"""
Test XXX: Test name and description

Detailed description of what this test validates.
Lists specific S3 features or behaviors being tested.
"""

from common.fixtures import TestFixture
from common.validators import validate_object_exists
from botocore.exceptions import ClientError

def test_XXX(s3_client, config):
    """Test function with descriptive docstring"""
    fixture = TestFixture(s3_client, config)
    bucket_name = None

    try:
        # Setup
        bucket_name = fixture.generate_bucket_name('test-XXX')
        s3_client.create_bucket(bucket_name)

        # Test logic
        # ... your test implementation ...

        # Assertions
        assert condition, "Descriptive error message"

    finally:
        # Cleanup
        if bucket_name and s3_client.bucket_exists(bucket_name):
            try:
                # Delete objects
                objects = s3_client.list_objects(bucket_name)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj['Key'])
                # Delete bucket
                s3_client.delete_bucket(bucket_name)
            except:
                pass
```

### Test Best Practices

1. **Always Clean Up**: Use try/finally to ensure resources are cleaned
2. **Unique Names**: Use fixture.generate_bucket_name() for unique names
3. **Clear Assertions**: Provide descriptive error messages
4. **Handle Errors**: Catch and handle expected errors appropriately
5. **Document Well**: Include comprehensive docstrings
6. **Validate Data**: Always verify data integrity for uploads/downloads

### Adding Test Categories

1. Create new directory under `tests/`
2. Follow numbering convention
3. Update `TEST_GROUPS` in `scripts/test-runner.py`
4. Document in this guide

## Performance Testing

### Running Performance Tests

```bash
# Basic performance benchmarks
make test GROUP=performance

# Extended performance testing
python scripts/test-runner.py \
  --config s3_config.yaml \
  --test 600 601 \
  --iterations 100 \
  --verbose
```

### Performance Metrics

Tests measure and report:

- **Latency Percentiles**: p50, p95, p99
- **Throughput**: MB/s for various object sizes
- **Operations/Second**: Under different concurrency levels
- **Resource Usage**: Connection pool, memory

### Interpreting Results

```
=== Sequential Performance Test ===
Testing small (100.0 KB)...
  Write: avg=0.045s, p50=0.043s, p95=0.052s, throughput=2.2 MB/s
  Read:  avg=0.023s, p50=0.022s, p95=0.027s, throughput=4.3 MB/s

=== Performance Summary ===
Total operations: 200
Average write latency: 0.123s
Average read latency: 0.087s
```

Key metrics to watch:
- Small object latency should be <100ms
- Large object throughput should be >10MB/s
- p99 latency should be <2x median

## Debugging Failed Tests

### Common Issues and Solutions

#### 1. Connection Errors
```
Error: Connection refused
```
**Solution**: Verify endpoint URL and port in s3_config.yaml

#### 2. Authentication Failures
```
Error: InvalidAccessKeyId
```
**Solution**: Check access credentials in configuration

#### 3. Timeout Errors
```
Error: Read timeout
```
**Solution**: Increase timeout in config or check network

#### 4. Resource Limits
```
Error: TooManyBuckets
```
**Solution**: Clean up test buckets or increase limits

### Debug Mode

```bash
# Enable debug output
export S3_DEBUG=1
make test TEST=004

# Save debug logs
make test TEST=004 2>&1 | tee debug.log

# Verbose test runner
python scripts/test-runner.py \
  --config s3_config.yaml \
  --test 004 \
  --verbose \
  --debug
```

### Test Isolation

Run single test in isolation:
```bash
# Direct Python execution
cd tests
python -m basic.004
```

## CI/CD Integration

### GitHub Actions

```yaml
name: S3 Compatibility Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          python -m venv venv
          source venv/bin/activate
          pip install -r requirements.txt

      - name: Start MinIO
        run: |
          docker run -d \
            -p 9000:9000 \
            -e MINIO_ROOT_USER=minioadmin \
            -e MINIO_ROOT_PASSWORD=minioadmin \
            --name minio \
            minio/minio server /data

      - name: Configure tests
        run: make defconfig-docker-demo

      - name: Run validation
        run: |
          source venv/bin/activate
          python scripts/production-validation.py \
            --config s3_config.yaml \
            --quick
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any

    stages {
        stage('Setup') {
            steps {
                sh 'python3 -m venv venv'
                sh '. venv/bin/activate && pip install -r requirements.txt'
            }
        }

        stage('Configure') {
            steps {
                sh 'make defconfig-docker-demo'
            }
        }

        stage('Test') {
            steps {
                sh '. venv/bin/activate && make test'
            }
        }

        stage('Validate Production') {
            steps {
                sh '''
                    . venv/bin/activate
                    python scripts/production-validation.py \
                        --config s3_config.yaml \
                        --output-dir results/${BUILD_NUMBER}
                '''
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'results/**/*'
            junit 'results/**/test-results.xml'
        }
    }
}
```

### Docker-based Testing

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Run tests
CMD ["python", "scripts/production-validation.py", \
     "--config", "s3_config.yaml"]
```

Build and run:
```bash
docker build -t msst-s3-tests .
docker run --rm \
  --network host \
  -v $(pwd)/results:/app/results \
  msst-s3-tests
```

## Test Configuration

### Environment Variables

```bash
# S3 endpoint configuration
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin

# Test behavior
export TEST_TIMEOUT=300
export TEST_RETRY_COUNT=3
export TEST_PARALLEL=10

# Debug options
export S3_DEBUG=1
export TEST_VERBOSE=1
```

### Configuration File Options

```yaml
# s3_config.yaml
s3_endpoint_url: "http://localhost:9000"
s3_access_key: "minioadmin"
s3_secret_key: "minioadmin"
s3_region: "us-east-1"
s3_use_ssl: false

# Test parameters
test_timeout: 60
test_retry_count: 3
test_object_sizes: "1KB,1MB,10MB,100MB"
test_bucket_prefix: "msst-test"

# Performance thresholds
performance_latency_p99: 1000  # ms
performance_throughput_min: 10  # MB/s
performance_ops_per_second: 100

# Debug settings
debug_enabled: true
debug_keep_buckets: false
debug_dump_requests: false
```

## Troubleshooting

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `NoSuchBucket` | Bucket doesn't exist | Check bucket creation succeeded |
| `BucketAlreadyExists` | Name collision | Use unique bucket names |
| `EntityTooSmall` | Part size <5MB | Increase multipart chunk size |
| `RequestTimeout` | Slow network/server | Increase timeout values |
| `ServiceUnavailable` | Server overload | Add retry logic |

### Getting Help

1. Check test output in `results/` directory
2. Enable debug mode for detailed logs
3. Review `validation-report.txt` for summary
4. Check specific test file for requirements
5. Consult `docs/PRODUCTION_TEST_PLAN.md` for test details

## Advanced Topics

### Custom Test Suites

Create custom test selections:
```bash
# Create custom suite file
cat > my-tests.txt <<EOF
004
005
006
100
600
EOF

# Run custom suite
for test in $(cat my-tests.txt); do
  python scripts/test-runner.py --config s3_config.yaml --test $test
done
```

### Parallel Test Execution

Run tests in parallel for faster execution:
```bash
# Using GNU parallel
cat my-tests.txt | parallel -j 4 \
  "python scripts/test-runner.py --config s3_config.yaml --test {}"

# Using xargs
cat my-tests.txt | xargs -P 4 -I {} \
  python scripts/test-runner.py --config s3_config.yaml --test {}
```

### Test Result Analysis

Analyze test results programmatically:
```python
import json
import glob

# Load all test results
results = []
for file in glob.glob('results/**/results.json'):
    with open(file) as f:
        results.append(json.load(f))

# Analyze pass rates
total = sum(r['summary']['total_tests'] for r in results)
passed = sum(r['summary']['passed'] for r in results)
print(f"Overall pass rate: {passed/total*100:.1f}%")
```