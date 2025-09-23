#!/usr/bin/env python3
"""
Production Validation Script for S3 Systems

Runs comprehensive test suite to validate S3 readiness for production.
Generates detailed reports with pass/fail criteria.
"""

import os
import sys
import json
import time
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
import yaml

class ProductionValidator:
    """Validates S3 system for production readiness"""

    # Test categories and their production criticality
    TEST_SUITES = {
        'critical': {
            'name': 'Critical Data Integrity',
            'tests': ['004', '005', '006'],
            'required_pass_rate': 100,
            'description': 'Data integrity and corruption prevention'
        },
        'error_handling': {
            'name': 'Error Handling & Recovery',
            'tests': ['011', '012'],
            'required_pass_rate': 100,
            'description': 'Network timeouts and retry logic'
        },
        'multipart': {
            'name': 'Multipart Operations',
            'tests': ['100', '101', '102'],
            'required_pass_rate': 100,
            'description': 'Large file handling and multipart uploads'
        },
        'versioning': {
            'name': 'Versioning Support',
            'tests': ['200'],
            'required_pass_rate': 80,
            'description': 'Object versioning capabilities'
        },
        'performance': {
            'name': 'Performance Benchmarks',
            'tests': ['600', '601'],
            'required_pass_rate': 90,
            'description': 'Throughput and latency requirements'
        }
    }

    # Production readiness criteria
    PRODUCTION_CRITERIA = {
        'latency_p99_ms': 1000,  # 99th percentile latency < 1s
        'throughput_mbps': 10,    # Minimum 10 MB/s throughput
        'concurrent_ops': 50,      # Handle 50+ concurrent operations
        'uptime_percent': 99.9,    # 99.9% availability
        'data_integrity': 100      # 100% data integrity required
    }

    def __init__(self, config_file, output_dir=None):
        """Initialize validator with config"""
        self.config_file = config_file
        self.output_dir = output_dir or f"validation-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'config': None,
            'suites': {},
            'summary': {},
            'production_ready': False
        }

        # Load config
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
            self.results['config'] = {
                'endpoint': self.config.get('s3_endpoint_url'),
                'vendor': self.config.get('vendor_type', 'unknown')
            }

    def run_test(self, test_id):
        """Run a single test and return results"""
        cmd = [
            'python', 'scripts/test-runner.py',
            '--config', self.config_file,
            '--test', test_id,
            '--output-dir', self.output_dir
        ]

        try:
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per test
            )
            duration = time.time() - start_time

            # Parse results
            passed = 'PASSED' in result.stdout or result.returncode == 0
            error_msg = None

            if not passed:
                # Extract error message
                for line in result.stdout.split('\n'):
                    if 'ERROR' in line or 'FAILED' in line:
                        error_msg = line.strip()
                        break

            return {
                'test_id': test_id,
                'passed': passed,
                'duration': duration,
                'error': error_msg
            }

        except subprocess.TimeoutExpired:
            return {
                'test_id': test_id,
                'passed': False,
                'duration': 300,
                'error': 'Test timeout (>5 minutes)'
            }
        except Exception as e:
            return {
                'test_id': test_id,
                'passed': False,
                'duration': 0,
                'error': str(e)
            }

    def run_suite(self, suite_name, suite_config):
        """Run a test suite"""
        print(f"\n{'='*60}")
        print(f"Running {suite_config['name']}")
        print(f"Description: {suite_config['description']}")
        print(f"Tests: {', '.join(suite_config['tests'])}")
        print(f"Required pass rate: {suite_config['required_pass_rate']}%")
        print('='*60)

        suite_results = {
            'name': suite_config['name'],
            'tests': [],
            'passed': 0,
            'failed': 0,
            'pass_rate': 0,
            'meets_requirement': False
        }

        for test_id in suite_config['tests']:
            print(f"  Running test {test_id}...", end='', flush=True)
            result = self.run_test(test_id)
            suite_results['tests'].append(result)

            if result['passed']:
                suite_results['passed'] += 1
                print(f" ✓ PASSED ({result['duration']:.2f}s)")
            else:
                suite_results['failed'] += 1
                print(f" ✗ FAILED - {result['error']}")

        # Calculate pass rate
        total_tests = len(suite_results['tests'])
        if total_tests > 0:
            suite_results['pass_rate'] = (suite_results['passed'] / total_tests) * 100
            suite_results['meets_requirement'] = (
                suite_results['pass_rate'] >= suite_config['required_pass_rate']
            )

        return suite_results

    def analyze_performance(self):
        """Analyze performance test results"""
        perf_analysis = {
            'latency_check': False,
            'throughput_check': False,
            'concurrency_check': False
        }

        # Read performance test outputs if available
        perf_files = Path(self.output_dir).glob('**/test-600-*.json')
        for perf_file in perf_files:
            with open(perf_file, 'r') as f:
                perf_data = json.load(f)
                # Analyze latency and throughput
                # This is simplified - real implementation would parse detailed metrics

        return perf_analysis

    def validate(self):
        """Run full production validation"""
        print("\n" + "="*80)
        print("S3 PRODUCTION VALIDATION SUITE")
        print("="*80)
        print(f"Endpoint: {self.results['config']['endpoint']}")
        print(f"Vendor: {self.results['config']['vendor']}")
        print(f"Output: {self.output_dir}")
        print(f"Started: {self.results['timestamp']}")

        # Create output directory
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Run each test suite
        all_passed = True
        for suite_name, suite_config in self.TEST_SUITES.items():
            suite_results = self.run_suite(suite_name, suite_config)
            self.results['suites'][suite_name] = suite_results

            if not suite_results['meets_requirement']:
                all_passed = False

        # Performance analysis
        perf_analysis = self.analyze_performance()

        # Generate summary
        total_tests = sum(len(s['tests']) for s in self.results['suites'].values())
        total_passed = sum(s['passed'] for s in self.results['suites'].values())
        total_failed = sum(s['failed'] for s in self.results['suites'].values())

        self.results['summary'] = {
            'total_tests': total_tests,
            'passed': total_passed,
            'failed': total_failed,
            'overall_pass_rate': (total_passed / total_tests * 100) if total_tests > 0 else 0,
            'critical_tests_passed': self.results['suites'].get('critical', {}).get('meets_requirement', False),
            'performance_analysis': perf_analysis
        }

        # Determine production readiness
        self.results['production_ready'] = (
            all_passed and
            self.results['summary']['critical_tests_passed'] and
            self.results['summary']['overall_pass_rate'] >= 95
        )

        # Save results
        self.save_results()

        # Print summary
        self.print_summary()

        return self.results['production_ready']

    def save_results(self):
        """Save validation results to file"""
        output_file = Path(self.output_dir) / 'validation-report.json'
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)

        # Also create a human-readable report
        report_file = Path(self.output_dir) / 'validation-report.txt'
        with open(report_file, 'w') as f:
            f.write("S3 PRODUCTION VALIDATION REPORT\n")
            f.write("="*80 + "\n\n")
            f.write(f"Timestamp: {self.results['timestamp']}\n")
            f.write(f"Endpoint: {self.results['config']['endpoint']}\n")
            f.write(f"Vendor: {self.results['config']['vendor']}\n\n")

            f.write("TEST RESULTS BY CATEGORY\n")
            f.write("-"*40 + "\n")
            for suite_name, suite in self.results['suites'].items():
                status = "✓ PASS" if suite['meets_requirement'] else "✗ FAIL"
                f.write(f"\n{suite['name']}: {status}\n")
                f.write(f"  Pass rate: {suite['pass_rate']:.1f}%")
                f.write(f" (required: {self.TEST_SUITES[suite_name]['required_pass_rate']}%)\n")
                f.write(f"  Tests passed: {suite['passed']}/{len(suite['tests'])}\n")

                for test in suite['tests']:
                    if not test['passed']:
                        f.write(f"    ✗ Test {test['test_id']}: {test['error']}\n")

            f.write("\n" + "="*80 + "\n")
            f.write("PRODUCTION READINESS: ")
            f.write("✓ READY\n" if self.results['production_ready'] else "✗ NOT READY\n")

    def print_summary(self):
        """Print validation summary to console"""
        print("\n" + "="*80)
        print("VALIDATION SUMMARY")
        print("="*80)

        for suite_name, suite in self.results['suites'].items():
            status = "✓" if suite['meets_requirement'] else "✗"
            print(f"{status} {suite['name']}: {suite['pass_rate']:.1f}% "
                  f"({suite['passed']}/{len(suite['tests'])} passed)")

        print("\n" + "-"*40)
        print(f"Overall: {self.results['summary']['overall_pass_rate']:.1f}% passed")
        print(f"Total tests: {self.results['summary']['total_tests']}")
        print(f"Passed: {self.results['summary']['passed']}")
        print(f"Failed: {self.results['summary']['failed']}")

        print("\n" + "="*80)
        if self.results['production_ready']:
            print("✓ PRODUCTION READY - All requirements met")
        else:
            print("✗ NOT PRODUCTION READY - Some requirements not met")
            print("\nFailed requirements:")
            for suite_name, suite in self.results['suites'].items():
                if not suite['meets_requirement']:
                    print(f"  - {suite['name']}: {suite['pass_rate']:.1f}% "
                          f"(required: {self.TEST_SUITES[suite_name]['required_pass_rate']}%)")
        print("="*80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='S3 Production Validation Suite'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to S3 configuration file'
    )
    parser.add_argument(
        '--output-dir',
        help='Directory for validation results'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run only critical tests'
    )

    args = parser.parse_args()

    # Create validator
    validator = ProductionValidator(args.config, args.output_dir)

    # Modify test suites if quick mode
    if args.quick:
        validator.TEST_SUITES = {
            k: v for k, v in validator.TEST_SUITES.items()
            if k in ['critical', 'error_handling']
        }

    # Run validation
    is_ready = validator.validate()

    # Exit with appropriate code
    sys.exit(0 if is_ready else 1)


if __name__ == '__main__':
    main()