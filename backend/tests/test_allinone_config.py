#!/usr/bin/env python3
"""
Test script to verify All-In-One container configuration.

This test verifies:
1. Supervisor configuration is valid
2. Redis defaults to localhost
3. Celery defaults to localhost
4. Environment variables are properly configured
"""

import os
import sys
import configparser
from pathlib import Path

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Get the repository root directory (3 levels up from this file)
REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = REPO_ROOT / "backend"

def test_supervisor_config():
    """Test that supervisor configuration is valid."""
    print("Testing supervisor configuration...")
    
    config_path = BACKEND_DIR / "supervisord.conf"
    
    if not os.path.exists(config_path):
        print(f"❌ FAIL: Supervisor config not found at {config_path}")
        return False
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Check required sections
    required_sections = ['supervisord', 'program:redis', 'program:celery-worker', 'program:flask-api']
    for section in required_sections:
        if section not in config.sections():
            print(f"❌ FAIL: Missing section '{section}' in supervisor config")
            return False
    
    # Check Redis configuration
    redis_config = config['program:redis']
    if 'command' not in redis_config or 'redis-server' not in redis_config['command']:
        print("❌ FAIL: Redis command not properly configured")
        return False
    
    # Check Celery worker configuration
    celery_config = config['program:celery-worker']
    if 'command' not in celery_config or 'celery' not in celery_config['command']:
        print("❌ FAIL: Celery worker command not properly configured")
        return False
    
    # Check Flask API configuration
    flask_config = config['program:flask-api']
    if 'command' not in flask_config or 'web_api.py' not in flask_config['command']:
        print("❌ FAIL: Flask API command not properly configured")
        return False
    
    print("✅ PASS: Supervisor configuration is valid")
    return True


def test_redis_defaults():
    """Test that Redis defaults to localhost in all components."""
    print("\nTesting Redis default configuration...")
    
    # Test celery_app.py
    celery_app_path = BACKEND_DIR / "celery_app.py"
    with open(celery_app_path, 'r') as f:
        celery_content = f.read()
        if "REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')" not in celery_content:
            print("❌ FAIL: celery_app.py does not default to localhost")
            return False
    
    # Test concurrency_manager.py
    concurrency_path = BACKEND_DIR / "concurrency_manager.py"
    with open(concurrency_path, 'r') as f:
        concurrency_content = f.read()
        if "redis_host = os.environ.get('REDIS_HOST', 'localhost')" not in concurrency_content:
            print("❌ FAIL: concurrency_manager.py does not default to localhost")
            return False
    
    # Test udi/redis_storage.py
    redis_storage_path = BACKEND_DIR / "udi" / "redis_storage.py"
    with open(redis_storage_path, 'r') as f:
        redis_storage_content = f.read()
        if "redis_host = os.environ.get('REDIS_HOST', 'localhost')" not in redis_storage_content:
            print("❌ FAIL: udi/redis_storage.py does not default to localhost")
            return False
    
    print("✅ PASS: All components default to localhost for Redis")
    return True


def test_dockerfile():
    """Test that Dockerfile includes required components."""
    print("\nTesting Dockerfile configuration...")
    
    dockerfile_path = REPO_ROOT / "Dockerfile"
    with open(dockerfile_path, 'r') as f:
        dockerfile_content = f.read()
    
    required_packages = ['redis-server', 'supervisor']
    for package in required_packages:
        if package not in dockerfile_content:
            print(f"❌ FAIL: Dockerfile does not install {package}")
            return False
    
    if 'supervisord.conf' not in dockerfile_content:
        print("❌ FAIL: Dockerfile does not copy supervisor configuration")
        return False
    
    if '/app/entrypoint.sh' not in dockerfile_content:
        print("❌ FAIL: Dockerfile does not use entrypoint.sh")
        return False
    
    print("✅ PASS: Dockerfile is properly configured for All-In-One")
    return True


def test_docker_compose():
    """Test that docker-compose.yml is configured for All-In-One."""
    print("\nTesting docker-compose.yml configuration...")
    
    compose_path = REPO_ROOT / "docker-compose.yml"
    with open(compose_path, 'r') as f:
        compose_content = f.read()
    
    # Check that only stream-checker service exists
    if 'redis:' in compose_content and 'image: redis:7-alpine' in compose_content:
        print("❌ FAIL: docker-compose.yml still has separate Redis service")
        return False
    
    if 'celery-worker:' in compose_content:
        print("❌ FAIL: docker-compose.yml still has separate Celery worker service")
        return False
    
    # Check that REDIS_HOST is set to localhost
    if 'REDIS_HOST=localhost' not in compose_content:
        print("❌ FAIL: docker-compose.yml does not set REDIS_HOST=localhost")
        return False
    
    print("✅ PASS: docker-compose.yml is properly configured for All-In-One")
    return True


def test_entrypoint():
    """Test that entrypoint.sh starts supervisor."""
    print("\nTesting entrypoint.sh configuration...")
    
    entrypoint_path = BACKEND_DIR / "entrypoint.sh"
    with open(entrypoint_path, 'r') as f:
        entrypoint_content = f.read()
    
    if 'supervisord' not in entrypoint_content:
        print("❌ FAIL: entrypoint.sh does not start supervisord")
        return False
    
    if 'All-In-One' not in entrypoint_content:
        print("❌ FAIL: entrypoint.sh does not mention All-In-One")
        return False
    
    print("✅ PASS: entrypoint.sh is properly configured")
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("All-In-One Container Configuration Tests")
    print("=" * 60)
    
    tests = [
        test_supervisor_config,
        test_redis_defaults,
        test_dockerfile,
        test_docker_compose,
        test_entrypoint,
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
