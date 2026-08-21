# Data Observability & Webhook Server

Production webhook server and observability platform for data pipelines (dbt Cloud, Airflow, MySQL, Snowflake, etc.).

---

## Repository Architecture

The project is divided into dedicated **`backend/`** and **`frontend/`** services:

```
web_hooks_server/
├── docker-compose.yml          # Single-command orchestration for full stack
├── .gitignore                  # Monorepo gitignore
├── README.md
│
├── backend/                    # Python Flask + Gunicorn WSGI Server (Port 5000)
│   ├── app.py                  # API endpoints, Swagger UI (/docs), Webhook handlers
│   ├── wsgi.py                 # Production WSGI entry point
│   ├── requirements.txt        # Backend dependencies
│   ├── Dockerfile              # Container definition for backend
│   ├── .dockerignore
│   ├── .env.example
│   ├── adapters/               # Log, Source, and Target data adapters
│   ├── config/                 # DB initialization, RDS MySQL & SQLite schemas
│   ├── shared/                 # Data models and shared dataclasses
│   ├── results/                # Saved telemetry JSON bundles
│   ├── seed_config.py          # Pipeline configuration seeder
│   └── setup_mysql.py          # Central MySQL DB setup script
│
└── frontend/                   # Web Observability Dashboard (Port 80)
    ├── index.html              # Primary Vithi Observability Dashboard UI
    ├── vithi_dashboard.html    # Standalone view
    ├── legacy_index.html       # Legacy portal view
    ├── nginx.conf              # Nginx reverse proxy configuration
    ├── Dockerfile              # Lightweight Nginx container
    └── .dockerignore
```

---

## Running with Docker Compose (Recommended)

To build and run both Backend and Frontend containers together:

```bash
# 1. (Optional) Configure environment variables
cp .env.example .env

# 2. Build and start full stack
docker compose up --build -d
```

- **Frontend Dashboard**: `http://localhost/` (Port 80)
- **Backend API & Swagger Docs**: `http://localhost:5000/docs`
- **Backend Health Check**: `http://localhost:5000/health`

---

## Standalone Backend Setup

```bash
cd backend

# 1. Install dependencies
pip install -r requirements.txt

# 2. Setup environment variables
cp .env.example .env

# 3. Seed pipeline configs (Optional)
python seed_config.py

# 4. Start backend server
python app.py
# Or with Gunicorn:
gunicorn --workers 3 --bind 0.0.0.0:5000 wsgi:app
```

The backend starts on `http://localhost:5000`.

---

## Standalone Frontend Setup

The frontend static assets can be opened directly or served via any static web server:

```bash
cd frontend

# Option A: Python HTTP server
python -m http.server 3000

# Option B: Docker container
docker build -t webhook-frontend .
docker run -p 80:80 webhook-frontend
```

---

## API Reference

| Method | Route | Description |
|--------|-------|-------------|
| `GET` | `/health` | Service health status and registered adapter list |
| `GET` | `/docs` | Interactive Swagger UI API documentation |
| `GET` | `/openapi.json` | OpenAPI 3.0 specification |
| `POST` | `/webhooks/dbt/<user_id>` | Receives dbt Cloud webhook and captures snapshots |
| `POST` | `/admin/register-config` | Register or update pipeline configuration |
| `GET` | `/admin/list-configs` | List all registered pipelines |
| `DELETE`| `/admin/delete-config/<job_id>` | Delete a pipeline configuration |
| `GET` | `/admin/runs` | List recent pipeline executions |
| `GET` | `/admin/runs/<id>` | Detailed telemetry for a specific pipeline execution |
| `GET` | `/api/dashboard/summary` | Executive summary KPI statistics |
| `GET` | `/api/dashboard/recent-runs` | Paginated live pipeline table |
| `GET` | `/api/dashboard/observability-details` | Observability & test metrics |
| `GET` | `/api/dashboard/quality-details` | Quality check breakdown |
| `GET` | `/api/dashboard/incidents` | Incident reports |
| `GET` | `/api/dashboard/lineage` | Dependency and lineage graph data |
