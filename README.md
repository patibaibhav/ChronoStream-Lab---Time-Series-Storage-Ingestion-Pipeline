# Time-Series Storage & Ingestion Pipeline Virtual Lab

A local-first virtual lab for exploring time-series ingestion, bounded buffering, chunking, and Parquet storage. The project is being delivered in small, reviewable tasks; this initial scaffold provides only runnable frontend and backend placeholders.

## Prerequisites

- Git
- Python 3.11 or newer (tested with Python 3.13)
- Node.js LTS and npm (tested with Node.js 24 and npm 11)

AWS credentials are not needed for local development.

## Docker Compose

Docker Compose is the standard local startup path. It runs the frontend at `http://localhost:5173` and the backend at `http://localhost:8000`.

```powershell
docker compose config
docker compose up --build
```

The backend receives `DATA_DIR=/app/data`, which is bind-mounted to `./data` on the host. SQLite metadata and future Parquet artifacts therefore survive `docker compose down` and container recreation. The browser uses same-origin `/api` requests; Nginx proxies those requests to the backend inside the Compose network, so no Docker-only hostname is exposed to browser code.

To verify bind-mount persistence after Docker is running:

```powershell
New-Item -ItemType Directory -Force .\data
Set-Content .\data\docker-persistence-check.txt "persistent"
docker compose down
docker compose up --build -d
Get-Content .\data\docker-persistence-check.txt
docker compose down
```

`data/` is intentionally ignored by Git. Remove the check file manually when finished.

## Local setup

Copy the environment template if you need to customize local values:

```powershell
Copy-Item .env.example .env
```

Create and activate a virtual environment, then install the backend and its test dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r .\backend\requirements.lock
python -m pip install -e .\backend --no-deps
```

Start the backend placeholder from the repository root:

```powershell
python -m uvicorn app.main:app --reload --app-dir backend
```

In a second terminal, install and start the frontend placeholder:

```powershell
Set-Location frontend
npm install
npm run dev
```

The frontend is available at `http://localhost:5173`. The backend listens at `http://127.0.0.1:8000`.

## Checks

```powershell
python -m pytest backend/tests

Set-Location frontend
npm run lint
npm run test
npm run build
```

## Repository layout

```text
backend/     Python package and backend tests
frontend/    React + TypeScript + Vite application and tests
docs/        Project documentation added with the relevant task
data/        Local databases and generated artifacts (gitignored)
```

`data/`, local databases, Parquet artifacts, exports, virtual environments, and `.env` files are intentionally excluded from Git. The future simulation engine, APIs, database schema, and interactive lab UI are deliberately out of scope for this scaffold.
