#!/usr/bin/env python3
"""
Test script to verify container configuration.

This test verifies:
1. Environment variables are properly configured
2. Dockerfile is properly configured
3. Entrypoint script is properly configured
"""

import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Get the repository root directory (3 levels up from this file)
REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = REPO_ROOT / "backend"


def test_dockerfile():
    """Test that Dockerfile is properly configured."""
    print("Testing Dockerfile configuration...")
    
    dockerfile_path = REPO_ROOT / "Dockerfile"
    with open(dockerfile_path, 'r') as f:
        dockerfile_content = f.read()
    
    # Check that supervisor is NOT installed (we removed it)
    if 'supervisor' in dockerfile_content.lower():
        print("❌ FAIL: Dockerfile should not install supervisor")
        return False
    
    # Check that redis-server is NOT installed (we don't use it)
    if 'redis-server' in dockerfile_content:
        print("❌ FAIL: Dockerfile should not install redis-server")
        return False
    
    if '/app/entrypoint.sh' not in dockerfile_content:
        print("❌ FAIL: Dockerfile does not use entrypoint.sh")
        return False
    
    print("✅ PASS: Dockerfile is properly configured")
    return True


def test_docker_compose():
    """Test that docker-compose.yml is configured correctly."""
    print("\nTesting docker-compose.yml configuration...")
    
    compose_path = REPO_ROOT / "docker-compose.yml"
    with open(compose_path, 'r') as f:
        compose_content = f.read()
    
    # Check that no separate Redis service exists
    if 'redis:' in compose_content or 'redis-server' in compose_content.lower():
        print("❌ FAIL: docker-compose.yml should not have Redis-related services or references")
        return False
    
    # Check that no separate Celery worker service exists
    if 'celery-worker:' in compose_content:
        print("❌ FAIL: docker-compose.yml should not have separate Celery worker service")
        return False
    
    print("✅ PASS: docker-compose.yml is properly configured")
    return True


def test_entrypoint():
    """Test that entrypoint.sh starts Flask API directly."""
    print("\nTesting entrypoint.sh configuration...")
    
    entrypoint_path = BACKEND_DIR / "entrypoint.sh"
    with open(entrypoint_path, 'r') as f:
        entrypoint_content = f.read()
    
    # Check that supervisord is NOT used
    if 'supervisord' in entrypoint_content:
        print("❌ FAIL: entrypoint.sh should not start supervisord")
        return False
    
    # Check that Flask API is started directly
    if 'python3 web_api.py' not in entrypoint_content:
        print("❌ FAIL: entrypoint.sh should start Flask API directly")
        return False
    
    # Check that exec is used (to make Flask PID 1)
    if 'exec python3 web_api.py' not in entrypoint_content:
        print("❌ FAIL: entrypoint.sh should use 'exec' to start Flask API")
        return False
    
    print("✅ PASS: entrypoint.sh is properly configured")
    return True


def test_no_supervisor_config():
    """Test that supervisord.conf does not exist."""
    print("\nTesting supervisord.conf removal...")
    
    config_path = BACKEND_DIR / "supervisord.conf"
    
    if config_path.exists():
        print("❌ FAIL: supervisord.conf should be removed")
        return False
    
    print("✅ PASS: supervisord.conf has been removed")
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("Container Configuration Tests")
    print("=" * 60)
    
    tests = [
        test_dockerfile,
        test_docker_compose,
        test_entrypoint,
        test_no_supervisor_config,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ FAIL: Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    print(f"Results: {sum(results)}/{len(results)} tests passed")
    print("=" * 60)
    
    if all(results):
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
