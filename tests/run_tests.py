#!/usr/bin/env python3
"""
Test runner script for REPACSS Power Profiling
Provides convenient commands for running different test suites
"""
import sys
import subprocess
import argparse
import os


def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"\n❌ {description} failed with exit code {result.returncode}")
        return False
    else:
        print(f"\n✅ {description} completed successfully")
        return True


def main():
    parser = argparse.ArgumentParser(description='REPACSS Test Runner')
    parser.add_argument('--type', choices=['unit', 'integration', 'e2e', 'all'], 
                       default='all', help='Type of tests to run')
    parser.add_argument('--coverage', action='store_true', 
                       help='Generate coverage report')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--parallel', '-n', type=int, default=1,
                       help='Number of parallel workers')
    parser.add_argument('--file', help='Run specific test file')
    
    args = parser.parse_args()
    
    # Base pytest command
    cmd = ['python', '-m', 'pytest']
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    
    # Add parallel execution
    if args.parallel > 1:
        cmd.extend(['-n', str(args.parallel)])
    
    # Add coverage
    if args.coverage:
        cmd.extend(['--cov=src', '--cov-report=html', '--cov-report=term'])
    
    # Determine test path
    if args.file:
        test_path = args.file
    elif args.type == 'unit':
        test_path = 'tests/unit/'
    elif args.type == 'integration':
        test_path = 'tests/integration/'
    elif args.type == 'e2e':
        test_path = 'tests/e2e/'
    else:  # all
        test_path = 'tests/'
    
    cmd.append(test_path)
    
    # Run the tests
    success = run_command(cmd, f"{args.type.title()} tests")
    
    if args.coverage and success:
        print(f"\n📊 Coverage report generated: htmlcov/index.html")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
