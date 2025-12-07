#!/bin/bash

# StreamFlow Entrypoint
# Starts Flask API in a single container

set -e

echo "[INFO] Starting StreamFlow Container: $(date)"

# Environment variables with defaults
API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-5000}"
DEBUG_MODE="${DEBUG_MODE:-false}"
CONFIG_DIR="${CONFIG_DIR:-/app/data}"

# Export environment variables for supervisor programs
export API_HOST API_PORT DEBUG_MODE CONFIG_DIR

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

# Start StreamFlow service
echo "[INFO] ============================================"
echo "[INFO] Starting StreamFlow Container"
echo "[INFO] ============================================"
echo "[INFO] Flask API: ${API_HOST}:${API_PORT}"
echo "[INFO] Debug mode: ${DEBUG_MODE}"
echo "[INFO] ============================================"
echo "[INFO] Access the web interface at http://localhost:${API_PORT}"
echo "[INFO] API documentation available at http://localhost:${API_PORT}/api/health"
echo "[INFO] ============================================"

# Start supervisor in foreground mode (nodaemon=true)
# This will become PID 1 and properly forward all logs to Docker stdout
echo "[INFO] Starting supervisor to manage Flask API..."

# Note: Supervisor will run in foreground and manage all processes
# All logs will be forwarded to stdout/stderr automatically
# Use exec to ensure supervisor becomes PID 1 and receives signals properly
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
