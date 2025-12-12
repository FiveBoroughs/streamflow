# Local Development Setup

This guide will help you set up a local development environment for StreamFlow with hot-reload capabilities.

## Prerequisites

- Docker and Docker Compose installed
- Git installed
- (Optional) Node.js 18+ for running frontend locally without Docker

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/krinkuto11/streamflow.git
   cd streamflow
   ```

2. **Create environment file**
   ```bash
   cp .env.template .env
   # Edit .env with your Dispatcharr credentials
   ```

3. **Start development environment**
   ```bash
   docker compose -f docker-compose.dev.yml up
   ```

4. **Access the application**
   - Frontend (with hot-reload): http://localhost:3000
   - Backend API: http://localhost:5000/api
   - The frontend will proxy API requests to the backend

## Development Workflow

### Working with Frontend

The frontend runs with Vite's hot-reload, so changes to React components will be reflected immediately in the browser.

**File structure:**
```
frontend/
├── src/
│   ├── components/     # Reusable UI components
│   │   ├── ui/         # ShadCN UI components
│   │   └── layout/     # Layout components (Sidebar, etc.)
│   ├── pages/          # Route pages (Dashboard, StreamChecker, etc.)
│   ├── hooks/          # Custom React hooks
│   ├── lib/            # Utility functions
│   ├── services/       # API client
│   └── App.jsx         # Main app component
├── context/            # Original MUI-based UI (for reference)
└── vite.config.js      # Vite configuration
```

**To make changes:**
1. Edit files in `frontend/src/`
2. Changes auto-reload in browser at http://localhost:3000
3. Check console for any errors

**Adding new ShadCN components:**
```bash
cd frontend
npx shadcn@latest add <component-name>
```

### Working with Backend

The backend Python code is mounted as a volume, allowing you to make changes that will be picked up on restart.

**To apply backend changes:**
1. Edit files in `backend/`
2. Restart the backend container:
   ```bash
   docker compose -f docker-compose.dev.yml restart backend
   ```

### Viewing Logs

```bash
# All services
docker compose -f docker-compose.dev.yml logs -f

# Backend only
docker compose -f docker-compose.dev.yml logs -f backend

# Frontend only
docker compose -f docker-compose.dev.yml logs -f frontend
```

### Stopping the Environment

```bash
# Stop all services
docker compose -f docker-compose.dev.yml down

# Stop and remove volumes (clean slate)
docker compose -f docker-compose.dev.yml down -v
```

## Alternative: Running Frontend Locally (Without Docker)

For faster frontend development, you can run the frontend directly on your host:

1. **Start only the backend in Docker:**
   ```bash
   docker compose -f docker-compose.dev.yml up backend
   ```

2. **Install frontend dependencies:**
   ```bash
   cd frontend
   npm install
   ```

3. **Run frontend dev server:**
   ```bash
   npm run dev
   ```

4. **Access:**
   - Frontend: http://localhost:3000
   - Backend: http://localhost:5000/api

The Vite dev server is configured to proxy API requests to `http://localhost:5000`.

## Building for Production

### Frontend Only
```bash
cd frontend
npm run build
# Output will be in frontend/build/
```

### Full Production Build
```bash
# Build frontend first
cd frontend
npm run build
cd ..

# Build Docker image
docker build -t streamflow:local .
```

### Test Production Build Locally
```bash
docker compose up
```

This uses the production `docker-compose.yml` with the locally built image.

## Troubleshooting

### Port Already in Use

If you get "port already in use" errors:

```bash
# Check what's using the port
sudo lsof -i :3000
sudo lsof -i :5000

# Stop the process or change ports in docker-compose.dev.yml
```

### Frontend Not Hot-Reloading

1. Check that files are being watched:
   ```bash
   docker compose -f docker-compose.dev.yml logs -f frontend
   ```

2. On Linux, you may need to increase inotify watches:
   ```bash
   echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
   sudo sysctl -p
   ```

### Backend Changes Not Applying

Restart the backend container:
```bash
docker compose -f docker-compose.dev.yml restart backend
```

### Database/Config Issues

Delete the data volume and restart:
```bash
docker compose -f docker-compose.dev.yml down -v
docker compose -f docker-compose.dev.yml up
```

## Project Structure

```
streamflow/
├── backend/              # Python Flask backend
│   ├── web_api.py        # Main API endpoints
│   ├── automated_stream_manager.py
│   ├── stream_checker_service.py
│   ├── requirements.txt
│   └── Dockerfile.dev    # Development Dockerfile
├── frontend/             # React + ShadCN UI
│   ├── src/              # Source code
│   ├── context/          # Original MUI UI (preserved)
│   ├── package.json
│   ├── vite.config.js
│   └── Dockerfile.dev    # Development Dockerfile
├── docker-compose.yml        # Production compose
├── docker-compose.dev.yml    # Development compose
├── Dockerfile                # Production Dockerfile
└── .env.template             # Environment template
```

## Technology Stack

### Frontend
- **Framework:** React 18
- **Build Tool:** Vite
- **UI Library:** ShadCN UI (Radix UI + Tailwind CSS)
- **Routing:** React Router v6
- **HTTP Client:** Axios
- **Charts:** Recharts

### Backend
- **Framework:** Flask (Python)
- **Task Queue:** Celery
- **Cache:** Redis
- **Process Manager:** Supervisor

## Tips for Development

1. **Use the browser DevTools** - React and Vite extensions are very helpful
2. **Check the console** - Vite shows compilation errors and warnings
3. **Hot-reload is fast** - No need to manually refresh after saving files
4. **API proxy works automatically** - Requests to `/api/*` go to backend
5. **Use dark mode** - The UI is designed with a dark theme by default

## Next Steps

- Implement remaining pages (StreamChecker, ChannelConfiguration, etc.)
- Add more ShadCN components as needed
- Write tests for new components
- Update documentation

## Support

For issues or questions:
- Check the main [README.md](../README.md)
- Review [docs/](../docs/) for detailed documentation
- Open an issue on GitHub
