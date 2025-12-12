# Context UI (Material-UI Based)

This directory contains the original Material-UI based UI implementation of StreamFlow.

## Purpose

This UI is preserved for compatibility and can be used in other environments or as a fallback.

## Structure

- `src/` - Original React components using Material-UI
  - `App.js` - Main application component with routing and sidebar
  - `components/` - All UI components (Dashboard, ChannelConfiguration, etc.)
  - `services/` - API client services
- `public/` - Static assets

## Technologies Used

- React 18
- Material-UI (MUI) v5
- React Router DOM v6
- Axios for API calls
- Recharts for visualizations

## Usage

To use this UI in a different environment:
1. Copy the contents of this folder to a new React project
2. Install dependencies from the main package.json
3. Ensure the backend API is accessible at `/api`
4. Run with `npm start` (Create React App)

## API Integration

The UI expects a Flask backend serving API endpoints at `/api`. See `services/api.js` for the complete API interface.
