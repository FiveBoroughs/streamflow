# Deployment Guide

## 🚀 All-In-One Single-Container Deployment

The application uses an **All-In-One single-container architecture** that is much easier to deploy and maintain:

- **Single Container**: All services in one Docker container managed by Supervisor
- **Embedded Redis**: Redis server runs locally within the container
- **Embedded Celery**: Celery workers run within the container for concurrent stream checking
- **Single Port**: Everything accessible at http://localhost:5000
- **No External Dependencies**: No need for separate Redis or worker containers
- **Faster Builds**: Pre-built frontend reduces Docker build time
- **Persistent Storage**: Configuration and Redis data stored in Docker volumes

## Quick Start

1. **Clone and configure**:
   ```bash
   git clone https://github.com/krinkuto11/streamflow.git
   cd streamflow
   cp .env.template .env
   # Edit .env with your Dispatcharr instance details
   ```

2. **Deploy with Docker Compose**:
   ```bash
   docker compose up -d
   ```

3. **Access the application**:
   - **Web Interface**: http://localhost:5000
   - **API Endpoints**: http://localhost:5000/api/health
   - **Health Check**: `curl http://localhost:5000/api/health`

## Data Persistence

All configuration files are stored in a Docker volume to ensure persistence across container restarts and updates.

### Volume Mapping

The default volume mapping in `docker-compose.yml`:
```yaml
volumes:
  - /srv/docker/databases/stream_checker:/app/data
```

This maps the host directory `/srv/docker/databases/stream_checker` to the container's `/app/data` directory.

### Persisted Configuration Files

The following files are stored in the mounted volume:
- `automation_config.json` - Automation system settings
- `channel_regex_config.json` - Regex patterns for stream assignment
- `changelog.json` - Activity history
- `stream_checker_config.json` - Stream quality checking configuration
- `channel_updates.json` - Channel update tracking
- `dump.rdb` - Redis database snapshot (auto-saved)

### Customizing the Volume Path

You can customize the host path by editing `docker-compose.yml`:
```yaml
volumes:
  - /your/custom/path:/app/data
```

Ensure the directory exists and has proper permissions:
```bash
mkdir -p /your/custom/path
chmod 755 /your/custom/path
```

## Architecture

The All-In-One application uses a single Docker container that includes:
- **Redis Server**: Embedded Redis for task queue and data storage (localhost:6379)
- **Celery Workers**: 4 concurrent workers for stream checking
- **Backend**: Python Flask API (port 5000)
- **Frontend**: React application served by Flask
- **Static Files**: React build served from Flask `/static` directory
- **Configuration**: JSON files in `/app/data` (mounted volume)
- **Process Manager**: Supervisor manages all services within the container

### Port Mapping
- **External**: http://localhost:5000 → **Internal**: Flask port 5000
- All frontend routes and API calls go through the same port
- Redis (localhost:6379) and Celery workers are internal only

## Docker Deployment Options

### Option 1: Quick Deploy (Recommended)

1. Create a `.env` file with your configuration:
   ```bash
   cp .env.template .env
   # Edit .env with your values
   ```

2. Deploy:
   ```bash
   docker compose up -d
   ```

### Option 2: Building with Pre-built Images from GHCR

1. Create a `.env` file with your configuration:
   ```bash
   cp .env.template .env
   # Edit .env with your values
   ```

2. Deploy using pre-built images:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d
   ```
   
   Note: Docker Compose will use the pre-built image from GHCR if available, or fall back to building locally if needed.

### Option 3: Building Locally

1. **Build the frontend** (if not already built):
   ```bash
   cd frontend
   npm install
   npm run build
   cd ..
   ```

2. **Create configuration**:
   ```bash
   cp .env.template .env
   # Edit .env with your values
   ```

3. **Build and deploy**:
   ```bash
   docker compose up -d --build
   ```

## Environment Variables

Environment variables can be configured in two ways:

### Option 1: Using .env file (Recommended)
All environment variables should be configured in the `.env` file:

- `DISPATCHARR_BASE_URL`: Base URL for Dispatcharr API
- `DISPATCHARR_USER`: Username for Dispatcharr
- `DISPATCHARR_PASS`: Password for Dispatcharr
- `DISPATCHARR_TOKEN`: JWT token for Dispatcharr API (auto-populated)
- `DEBUG_MODE`: Enable debug mode (true/false)
- `API_HOST`: API host (default: 0.0.0.0)
- `API_PORT`: API port (default: 5000)
- `CONFIG_DIR`: Configuration directory (default: /app/data)
- `REDIS_HOST`: Redis host (default: localhost for All-In-One)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)

### Option 2: Direct Environment Variables (Docker Compose)
For containerized deployments, you can specify environment variables directly in docker-compose.yml:

```yaml
services:
  stream-checker:
    # ... other config ...
    environment:
      - DISPATCHARR_BASE_URL=http://your-dispatcharr-instance.com:9191
      - DISPATCHARR_USER=your-username
      - DISPATCHARR_PASS=your-password
      - DEBUG_MODE=false
      - API_HOST=0.0.0.0
      - API_PORT=5000
      - CONFIG_DIR=/app/data
      - REDIS_HOST=localhost
      - REDIS_PORT=6379
      - REDIS_DB=0
```

> **Note**: When using direct environment variables, comment out or remove the `env_file` section in docker-compose.yml. JWT tokens will be refreshed automatically on startup as they cannot be persisted without a .env file.

## Accessing the Application

- **Web Interface & API**: http://localhost:5000
- All endpoints are accessible through the single port
- Redis and Celery workers are internal to the container

## GitHub Container Registry

Images are automatically built and pushed to GHCR via GitHub Actions when a new release is published. Images are available at:
- `ghcr.io/krinkuto11/streamflow:latest` - Latest release
- `ghcr.io/krinkuto11/streamflow:<version>` - Specific version (e.g., v1.0.0, v1.0.1)
- `ghcr.io/krinkuto11/streamflow:<major>.<minor>` - Major.minor version (e.g., 1.0)
- `ghcr.io/krinkuto11/streamflow:<major>` - Major version only (e.g., 1)

Note: Images are only pushed to GHCR when creating releases, not on every push or pull request. This ensures only tested and stable versions are published to the registry.

**Creating a Release**: See [Release Guide](../.github/RELEASE_GUIDE.md) for instructions on how to create and publish a release.
