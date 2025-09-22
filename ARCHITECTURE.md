# MSST-S3: Multi-vendor S3 Storage Test Suite

## Architecture Overview

MSST-S3 is a vendor-neutral S3 interoperability test suite that combines the best practices from Ceph s3-tests, MinIO mint, boto3 test frameworks, and follows the organizational patterns of xfstests/blktests.

## Design Principles

1. **Vendor Neutrality**: Test any S3-compatible storage system
2. **Itemized Tests**: Each test is independently numbered and runnable (like xfstests)
3. **Configuration-Driven**: Use kconfig for all test configuration
4. **Automation-First**: Ansible integration for deployment and orchestration
5. **Multi-SDK Testing**: Test the same operations across different S3 client libraries
6. **Comprehensive Coverage**: Cover basic operations to advanced S3 features

## Directory Structure

```
msst-s3/
├── Kconfig                     # Main configuration menu
├── Makefile                    # Primary build and test targets
├── Makefile.subtrees          # Git subtree management
├── scripts/
│   ├── kconfig/               # Kconfig implementation (git subtree)
│   ├── test-runner.py         # Main test execution framework
│   └── result-analyzer.py     # Test result analysis
├── tests/
│   ├── common/                # Shared test utilities
│   │   ├── __init__.py
│   │   ├── s3_client.py       # S3 client wrapper
│   │   ├── fixtures.py        # Test fixtures
│   │   └── validators.py      # Result validators
│   ├── basic/                 # Basic S3 operations (001-099)
│   ├── multipart/            # Multipart upload tests (100-199)
│   ├── versioning/           # Versioning tests (200-299)
│   ├── acl/                  # ACL tests (300-399)
│   ├── encryption/           # Encryption tests (400-499)
│   ├── lifecycle/            # Lifecycle tests (500-599)
│   ├── performance/          # Performance tests (600-699)
│   ├── stress/               # Stress tests (700-799)
│   └── compatibility/        # Vendor-specific tests (800-899)
├── workflows/
│   ├── s3-tests/
│   │   ├── Makefile          # S3 test workflow targets
│   │   └── Kconfig           # S3 test configuration
│   └── performance/
│       ├── Makefile          # Performance workflow targets
│       └── Kconfig           # Performance configuration
├── playbooks/
│   ├── s3-tests.yml          # Main S3 test playbook
│   ├── inventory/            # Ansible inventory
│   └── roles/
│       ├── s3-setup/         # S3 endpoint setup
│       ├── s3-tests/         # Test execution role
│       └── s3-results/       # Result collection
├── configs/                  # Pre-defined configurations
│   ├── aws.config           # AWS S3 configuration
│   ├── minio.config         # MinIO configuration
│   ├── ceph.config          # Ceph RGW configuration
│   └── gcs.config           # Google Cloud Storage
└── results/                  # Test results and reports
```

## Test Numbering Scheme

Following xfstests/blktests pattern:

- **001-099**: Basic operations (bucket/object CRUD)
- **100-199**: Multipart uploads
- **200-299**: Versioning
- **300-399**: Access control (ACL, policies)
- **400-499**: Encryption (SSE-S3, SSE-C, SSE-KMS)
- **500-599**: Lifecycle management
- **600-699**: Performance tests
- **700-799**: Stress tests
- **800-899**: Vendor-specific compatibility
- **900-999**: Reserved for future use

## Configuration System (Kconfig)

### Main Configuration Categories

1. **Target Configuration**
   - S3 endpoint URL
   - Authentication credentials
   - Region settings
   - TLS/SSL options

2. **Test Selection**
   - Test groups to run
   - Individual test selection
   - Skip lists for known failures

3. **Test Parameters**
   - Object sizes
   - Concurrency levels
   - Duration for stress tests
   - Performance thresholds

4. **Output Configuration**
   - Result format (JSON, YAML, JUnit)
   - Logging verbosity
   - Report generation

## Test Framework (Python)

### Core Components

1. **Test Runner** (`scripts/test-runner.py`)
   - Parse configuration from kconfig
   - Execute selected tests
   - Collect and format results
   - Handle test dependencies

2. **S3 Client Wrapper** (`tests/common/s3_client.py`)
   - Abstraction over boto3/other SDKs
   - Vendor-specific workarounds
   - Connection pooling
   - Retry logic

3. **Test Structure**
   ```python
   # tests/basic/001
   def test_001_bucket_create():
       """Create a simple bucket"""
       # Test implementation

   # tests/basic/002
   def test_002_bucket_list():
       """List buckets"""
       # Test implementation
   ```

## Ansible Integration

### Playbook Structure

1. **Setup Phase**
   - Configure S3 endpoints
   - Install dependencies
   - Validate connectivity

2. **Execution Phase**
   - Run selected test groups
   - Monitor progress
   - Handle failures

3. **Collection Phase**
   - Gather results
   - Generate reports
   - Archive artifacts

### Makefile Targets

```makefile
# Configuration
make menuconfig          # Interactive configuration
make defconfig          # Default configuration

# Testing
make test               # Run all enabled tests
make test-basic         # Run basic tests only
make test GROUP=acl     # Run specific test group
make test TEST=001      # Run specific test

# Ansible automation
make s3-deploy          # Deploy test infrastructure
make s3-run            # Run tests via ansible
make s3-results        # Collect and analyze results

# Maintenance
make refresh-kconfig    # Update kconfig subtree
make clean             # Clean build artifacts
```

## Test Execution Flow

1. **Configuration Loading**
   - Read .config from kconfig
   - Generate s3_config.yaml
   - Validate configuration

2. **Test Discovery**
   - Scan test directories
   - Filter by configuration
   - Build execution plan

3. **Test Execution**
   - Initialize S3 client
   - Run tests in order
   - Handle dependencies
   - Collect results

4. **Result Processing**
   - Format output (JSON/YAML/JUnit)
   - Generate summary report
   - Archive for analysis

## Multi-vendor Support

### Vendor Abstraction Layer

- Configuration profiles for each vendor
- Vendor-specific test markers
- Feature capability detection
- Workaround management

### Supported Implementations

1. **AWS S3** - Reference implementation
2. **MinIO** - Open source S3 compatible
3. **Ceph RGW** - Ceph RADOS Gateway
4. **Google Cloud Storage** - S3 compatibility mode
5. **Azure Blob Storage** - S3 API layer
6. **Wasabi** - S3 compatible cloud storage
7. **DigitalOcean Spaces** - S3 compatible object storage

## Performance Testing

### Metrics Collection

- Operation latency (p50, p95, p99)
- Throughput (MB/s, ops/s)
- Concurrency scaling
- Error rates

### Test Scenarios

- Single vs multipart upload performance
- Concurrent operation scaling
- Large object handling
- Small file performance

## Continuous Integration

### GitHub Actions Workflow

```yaml
- Run against multiple S3 implementations
- Test matrix for different configurations
- Result aggregation and reporting
- Regression detection
```

## Extensibility

### Adding New Tests

1. Create test file in appropriate category
2. Follow naming convention (e.g., tests/basic/042)
3. Update Kconfig if new options needed
4. Document in test catalog

### Adding New Vendors

1. Create configuration profile
2. Add to vendor detection logic
3. Document known limitations
4. Update CI matrix

## Dependencies

- Python 3.8+
- boto3
- pytest
- ansible
- make
- kconfig tools

## Future Enhancements

1. **Multi-SDK Testing**: Beyond boto3 (aws-sdk-go, minio-py, etc.)
2. **Compliance Testing**: S3 API specification validation
3. **Chaos Testing**: Failure injection and recovery
4. **Cost Analysis**: Operation cost estimation
5. **Security Testing**: Permission and encryption validation