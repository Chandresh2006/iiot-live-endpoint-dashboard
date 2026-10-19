# IIoT Live Endpoint Dashboard

NAME: CHANDRESH P & ROLL NO: 24EIR016

A live dashboard for Azure IoT Hub telemetry. It connects to the IoT Hub
Event Hub-compatible endpoint, shows every incoming payload as raw ASCII byte
values, and decodes the same bytes as text - updating in real time whenever
the device sends new data.

## Features

- Live consumption of telemetry from the Azure IoT Hub built-in endpoint
- Raw ASCII values view (decimal byte values as received)
- Decoded string view of the same payload
- Real-time updates via Server-Sent Events (SSE) - no page refresh
- Connection status, event counter and last event timestamp
- Graceful auto-reconnect to both the IoT Hub and the browser stream

## Architecture

```
backend/app.py        Flask server
                      - consumes events from the IoT Hub endpoint
                      - exposes /latest (JSON snapshot)
                      - exposes /stream (SSE live feed)
                      - serves the built React app

frontend/             React app (Vite)
  src/App.jsx         Dashboard UI (EventSource consumer)
  src/App.css         Styling
  vite.config.js      Dev server proxy to the Flask backend

.env                  IOTHUB_CONNECTION_STRING (never committed)
.env.example          Template for the environment file
requirements.txt      Python dependencies
```

Data flow: `IoT device -> IoT Hub endpoint -> backend (EventHubConsumerClient) -> SSE -> React UI`

## Prerequisites

- Python 3.10+
- Node.js 18+
- An Azure IoT Hub connection string (Event Hub-compatible endpoint, e.g. from
  the `iothubowner` shared access policy)

## Setup

1. Create the environment file and add your connection string:

```
copy .env.example .env
```

2. Install backend dependencies:

```
pip install -r requirements.txt
```

3. Install frontend dependencies and build:

```
cd frontend
npm install
npm run build
```

4. Start the server from the project root:

```
python backend/app.py
```

5. Open http://127.0.0.1:5000

## Development mode

Run the backend and the Vite dev server separately for hot reload:

```
python backend/app.py
cd frontend
npm run dev
```

Then open http://127.0.0.1:5173 (API and stream are proxied to the backend).

## Security notes

- The connection string lives only in `.env`, which is git-ignored.
- Rotate shared access keys periodically in the Azure Portal.
- Use a dedicated consumer group instead of `$Default` if other services
  consume from the same endpoint.
