# All-In-One build for StreamFlow application
# Includes Redis, Celery worker, and Flask API in a single container
# Frontend should be pre-built and copied to build context

FROM python:3.11-slim

# Install system dependencies including Redis and supervisor for process management
RUN apt-get update && apt-get install -y \
    curl \
    ffmpeg \
    redis-server \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Create working directory for backend
WORKDIR /app

# Copy backend requirements first for better caching
COPY backend/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Copy backend application code
COPY backend/ ./

# Copy pre-built frontend to static directory
COPY frontend/build ./static

# Create necessary directories
# data directory will be mounted as volume for persistence
RUN mkdir -p csv logs data

# Set environment variable for config directory
ENV CONFIG_DIR=/app/data

# Create supervisor configuration directory
RUN mkdir -p /etc/supervisor/conf.d

# Copy supervisor configuration
COPY backend/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Set permissions for entrypoint
RUN chmod +x entrypoint.sh

# Create default configuration files in the data directory
RUN python3 create_default_configs.py

# Create directory for Redis data
RUN mkdir -p /var/lib/redis && chown -R redis:redis /var/lib/redis || true

# Expose only the Flask port
EXPOSE 5000

# Health check for Flask (Redis and Celery are internal)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:5000/api/health || exit 1

# Use entrypoint script to start all services via supervisor
ENTRYPOINT ["/app/entrypoint.sh"]

