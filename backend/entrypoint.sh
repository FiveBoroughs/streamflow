#!/bin/bash

# StreamFlow All-In-One Entrypoint
# Starts Redis, Celery worker, and Flask API in a single container

set -e

echo "[INFO] Starting StreamFlow All-In-One Container: $(date)"

# Environment variables with defaults
API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-5000}"
DEBUG_MODE="${DEBUG_MODE:-false}"
CONFIG_DIR="${CONFIG_DIR:-/app/data}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"

# Export environment variables for supervisor programs
export API_HOST API_PORT DEBUG_MODE CONFIG_DIR REDIS_HOST REDIS_PORT REDIS_DB

# Deprecated: Old manual interval approach (kept for backward compatibility warnings)
if [ -n "$INTERVAL_SECONDS" ]; then
    echo "[WARNING] INTERVAL_SECONDS environment variable is deprecated."
    echo "[WARNING] The system now uses automated scheduling via the web API."
    echo "[WARNING] Please configure automation via the web interface or API endpoints."
fi

# Check if configuration files exist, create defaults if needed
echo "[INFO] Checking configuration files..."

# Ensure required directories exist (including the persisted data directory)
mkdir -p csv logs "$CONFIG_DIR"
echo "[INFO] Config directory: $CONFIG_DIR"

# Validate environment setup
if [ ! -f ".env" ]; then
    echo "[WARNING] No .env file found. Using environment variables for configuration."
    echo "[INFO] Ensure DISPATCHARR_BASE_URL, DISPATCHARR_USER, and DISPATCHARR_PASS are set."
    
    # Check if required environment variables are set
    if [ -z "$DISPATCHARR_BASE_URL" ] || [ -z "$DISPATCHARR_USER" ] || [ -z "$DISPATCHARR_PASS" ]; then
        echo "[ERROR] Required environment variables not found:"
        echo "[ERROR] DISPATCHARR_BASE_URL, DISPATCHARR_USER, and DISPATCHARR_PASS must be set"
        echo "[INFO] You can either:"
        echo "[INFO] 1. Set environment variables in docker-compose.yml, or"
        echo "[INFO] 2. Copy .env.template to .env and configure your settings."
        exit 1
    fi
else
    echo "[INFO] Using .env file for configuration."
fi

# Start All-In-One services
echo "[INFO] ============================================"
echo "[INFO] Starting All-In-One StreamFlow Container"
echo "[INFO] ============================================"
echo "[INFO] Redis: localhost:${REDIS_PORT}"
echo "[INFO] Celery Worker: 4 concurrent workers"
echo "[INFO] Flask API: ${API_HOST}:${API_PORT}"
echo "[INFO] Debug mode: ${DEBUG_MODE}"
echo "[INFO] ============================================"
echo "[INFO] Access the web interface at http://localhost:${API_PORT}"
echo "[INFO] API documentation available at http://localhost:${API_PORT}/api/health"
echo "[INFO] ============================================"

# Start supervisor in the background
echo "[INFO] Starting supervisor to manage all processes..."
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

# Wait for Redis to be ready before continuing
echo "[INFO] Waiting for Redis to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if redis-cli -h localhost -p ${REDIS_PORT} ping > /dev/null 2>&1; then
        echo "[INFO] Redis is ready!"
        break
    fi
    attempt=$((attempt + 1))
    if [ $attempt -lt $max_attempts ]; then
        echo "[INFO] Redis not ready yet, waiting... (attempt $attempt/$max_attempts)"
        sleep 1
    else
        echo "[WARNING] Redis did not become ready after $max_attempts seconds"
        echo "[WARNING] Services will continue but may experience initial connection issues"
    fi
done

# Wait for supervisor log file to be created
echo "[INFO] Waiting for supervisor log file..."
max_wait_attempts=10
wait_count=0
while [ $wait_count -lt $max_wait_attempts ]; do
    if [ -f /app/logs/supervisord.log ]; then
        echo "[INFO] Supervisor log file is ready!"
        break
    fi
    wait_count=$((wait_count + 1))
    sleep 1
done

# Create log file if it doesn't exist after waiting
if [ ! -f /app/logs/supervisord.log ]; then
    echo "[WARNING] Supervisor log file not found after $max_wait_attempts attempts"
    echo "[INFO] Creating temporary log file (will be populated by supervisord)"
    mkdir -p /app/logs
    touch /app/logs/supervisord.log
fi

# Keep the container running by tailing supervisor logs
# Use exec to ensure tail becomes PID 1 and receives signals properly
exec tail -f /app/logs/supervisord.log

