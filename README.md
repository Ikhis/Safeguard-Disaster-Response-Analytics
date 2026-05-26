# SafeGuard Disaster Response Analytics

A scalable, end-to-end data engineering pipeline that ingests real disaster data from the FEMA API, generates synthetic insurance claims, transforms raw data through structured layers, and delivers operational dashboards via Power BI.

Built for **SafeGuard Insurance** — a simulated insurance company — to demonstrate how modern data platforms can provide real-time operational visibility during disaster events.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Table of Contents

- [Business Context](#business-context)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Database Schema](#database-schema)
- [Dashboards](#dashboards)
- [Key Features](#key-features)
- [Future Enhancements](#future-enhancements)

---

## Business Context

During major disasters, insurance companies face critical challenges in monitoring incoming claims, identifying processing delays, prioritizing high-impact claims, and allocating resources across affected regions. Traditional reporting systems are often delayed, fragmented, and reactive.

**Disaster Response Analytics** addresses this gap by implementing a modern, scalable analytics architecture that transforms raw disaster and claims data into actionable intelligence, enabling insurance operations teams to track claims performance metrics, monitor backlog and resolution timelines, detect operational inefficiencies, and improve disaster response effectiveness.

---

## Architecture

![Data Pipeline Architecture](<architecture diagram.png>)
---

## Data Flow

The pipeline follows a three-layer medallion architecture:

**Raw Layer** — Data lands here exactly as received from the source. No transformations are applied. Dates remain as text strings, and the full API response is stored as JSONB for auditability. Tables: `raw.fema_disasters`, `raw.insurance_claims`.

**Staging Layer** — Data is cleaned, validated, and type-cast. Text dates become real DATE types, constraints enforce allowed values (claim types, statuses, priorities), and derived columns are calculated (`days_open`, `is_overdue`). Tables: `staging.disasters`, `staging.claims`.

**Curated Layer** — Pre-aggregated tables designed to feed dashboards directly. Each table answers a specific business question without requiring heavy computation at query time. Tables: `curated.disaster_summary`, `curated.claims_daily_metrics`, `curated.regional_performance`, `curated.backlog_aging`.

---

## Project Structure

```
disaster-response-analytics/
│
├── config/
│   ├── __init__.py
│   └── settings.py                 # Central configuration (DB URL, API endpoints, thresholds)
│
├── utils/
│   ├── __init__.py
│   ├── database.py                 # Connection management, batch inserts, pipeline metadata
│   └── logger.py                   # Dual-output logging (console + file)
│
├── etl/
│   ├── __init__.py
│   ├── run_pipeline.py             # Orchestrator — runs all steps or individual steps
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── fema_ingest.py          # FEMA API ingestion with pagination and retry logic
│   │   └── claims_generator.py     # Synthetic claims generation linked to real disasters
│   ├── staging/
│   │   ├── __init__.py
│   │   └── transform.py            # Raw → Staging transformations and validation
│   └── curated/
│       ├── __init__.py
│       └── aggregate.py            # Staging → Curated KPI aggregations
│
├── sql/
│   └── 001_init_schema.sql         # Full database schema (all three layers + metadata)
│
├── logs/                           # Pipeline execution logs
├── data/                           # Scratch space for local data files
│
├── docker-compose.yml              # PostgreSQL container configuration
├── Dockerfile                      # Python ETL container image
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template
├── .gitignore
└── README.md
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.11+ | ETL logic, API integration, data transformation |
| Database | PostgreSQL 16 | Structured storage across Raw, Staging, Curated layers |
| Containerization | Docker & Docker Compose | Portable database deployment |
| Visualization | Power BI | Interactive operational dashboards |
| API Source | FEMA Open API v2 | Real US disaster and funded project data |
| Key Libraries | psycopg2, requests, pandas, SQLAlchemy, Faker | DB connectivity, HTTP, data manipulation, test data |

---

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Docker & Docker Compose
- Power BI Desktop (for dashboards)

### 1. Setup Git/GitHub Repository

```bash
git init
git add .
git commit -m "first commit"
git branch -M main
git push -u origin main
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and set your password:

```
POSTGRES_PASSWORD=your_strong_password
DATABASE_URL=postgresql://dra_admin:your_strong_password@127.0.0.1:5433/disaster_response
FEMA_API_BASE_URL=https://www.fema.gov/api/open/v2
LOG_LEVEL=INFO
```

### 3. Start PostgreSQL

```bash
docker compose up -d postgres
```

This automatically creates the database, user, and all tables across three schemas.

### 4. Set Up Python Environment

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 5. Verify the Setup

```bash
# Check database tables
docker exec -it dra_postgres psql -U dra_admin -d disaster_response -c "\dt raw.*"

# Check Python connection
python -c "from utils.database import execute_query; rows = execute_query('SELECT current_database()', fetch=True); print('Connected to:', rows[0]['current_database'])"
```

---

## Running the Pipeline

### Full Pipeline (all steps)

```bash
python -m etl.run_pipeline
```

This executes four steps in sequence:

1. **FEMA Ingestion** — Fetches disaster-funded project data from the FEMA API
2. **Claims Generation** — Creates synthetic insurance claims linked to real disasters
3. **Staging Transformations** — Cleans, validates, and enriches the raw data
4. **Curated Aggregations** — Builds dashboard-ready KPI tables

### Individual Steps

```bash
python -m etl.run_pipeline fema       # Step 1 only
python -m etl.run_pipeline claims     # Step 2 only
python -m etl.run_pipeline staging    # Step 3 only
python -m etl.run_pipeline curated    # Step 4 only
```

### Pipeline Metadata

Every execution is logged to `public.pipeline_runs`:

```sql
SELECT pipeline_name, status, rows_ingested, started_at
FROM public.pipeline_runs
ORDER BY started_at DESC;
```

---

## Database Schema

### Raw Layer (append-only, no transformations)

**`raw.fema_disasters`** — FEMA PublicAssistanceFundedProjectsDetails records including disaster number, incident type, state, county, project amounts, federal obligations, and full JSON payload.

**`raw.insurance_claims`** — Synthetic insurance claim records with claim ID, policy ID, disaster linkage, claimant details, amounts, dates, status, and priority.

### Staging Layer (cleaned, validated, enriched)

**`staging.disasters`** — Deduplicated disasters with proper DATE types, normalized state codes, and validated fields. One row per disaster number.

**`staging.claims`** — Validated claims with enforced constraints on claim type, status, and priority. Includes derived columns: `days_open` (days since filing) and `is_overdue` (true if exceeding 30-day SLA target).

### Curated Layer (aggregated KPIs)

**`curated.disaster_summary`** — Per-disaster KPIs: total/open/closed/approved/denied claims, total and average amounts, average resolution time, SLA compliance percentage.

**`curated.claims_daily_metrics`** — Daily claims activity by region: filed, resolved, open, overdue counts with rolling SLA compliance.

**`curated.regional_performance`** — Regional scorecards: claim volumes, overdue counts, average resolution days, top disaster type per region.

**`curated.backlog_aging`** — Open claims grouped by age buckets (0–7, 8–14, 15–30, 31–60, 60+ days) per region.

---

## Dashboards

Three Power BI dashboard pages connected to the curated layer via PostgreSQL:

### Page 1 — Executive Overview
KPI cards (total claims, open claims, closed claims, SLA compliance), claims filed vs resolved over time, claims distribution by region, and a top disasters table.

**Executive Summary Dashboard**  ![image here](./img/Dashboard_Executive_Summary.png)

### Page 2 — Claims Operation
Backlog aging by region, SLA compliance by region, overdue claims distribution, and a regional performance scorecard table.

**Claims Operation Dashboard**  ![image here](./img/Dashboard_Claim_Operation.png)

### Page 3 — Disaster Impact
Total claim amounts by incident type, geographic map of claims by state with incident type color coding, and a disaster timeline showing claims trends over time.

**Disaster Impact Dashboard**  ![image here](./img/Dashboard_Disaster_Impact.png)

### Connecting Power BI

1. Open Power BI Desktop → Get Data → PostgreSQL database
2. Server: `127.0.0.1:5433` | Database: `disaster_response`
3. Authenticate with `dra_admin` and your password
4. Select tables from the `curated` schema
5. Build visuals using the dashboard layouts described above

---

## Key Features

**Incremental Loading** — The FEMA ingestion pipeline tracks previously loaded records by hash and skips duplicates on subsequent runs, ensuring efficient re-execution.

**API data Ingestion**  ![image here](./img/FEMA_API_data_ingested_into_etl_pipeline.png)

**Fault Tolerance** — API calls include retry logic with exponential backoff (2s, 4s, 8s). Failed pipeline runs are logged with error details for debugging.

**Idempotent Transformations** — Staging and curated layers use UPSERT (ON CONFLICT DO UPDATE) operations, making re-runs safe without data duplication.

**Claims Data in Staging**  ![image here](./img/Claims_table_in_staging.png)

**Disaster Data in Staging**  ![image here](./img/Disaster_table_in_staging.png)

**Pipeline Observability** — Every pipeline execution is recorded in `public.pipeline_runs` with status, row counts, timing, and error messages.

**Pipeline Orchestration**  ![image here](./img/Pipeline_Orchestration.png)

**Configurable Thresholds** — SLA targets, batch sizes, page limits, and claims-per-disaster ranges are centralized in `config/settings.py`.

**Realistic Test Data** — The claims generator uses weighted distributions for claim types, statuses, and priorities, with amount ranges calibrated to real-world insurance patterns.

**Generated Claim Data**  ![image here](./img/Insurance_claim_generated.png)

---

## Future Enhancements

- **Scheduling** — Add Apache Airflow DAGs or cron jobs for automated pipeline runs
- **Real Claims Integration** — Replace synthetic generator with CDC (Change Data Capture) from a production claims database
- **Data Quality** — Add Great Expectations or dbt tests for validation and monitoring
- **Alerting** — Slack or email notifications on pipeline failures or SLA breaches
- **Table Partitioning** — Partition claims tables by date for improved query performance at scale
- **Cloud Migration** — Move PostgreSQL to AWS RDS or Azure Database for production deployment
- **Additional FEMA Endpoints** — Incorporate DisasterDeclarationsSummaries for richer disaster metadata

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `POSTGRES_PASSWORD` | PostgreSQL password | `your_strong_password` |
| `DATABASE_URL` | Full connection string | `postgresql://dra_admin:pwd@127.0.0.1:5433/disaster_response` |
| `FEMA_API_BASE_URL` | FEMA API base URL | `https://www.fema.gov/api/open/v2` |
| `LOG_LEVEL` | Logging verbosity | `INFO`, `DEBUG`, `WARNING` |

---

## Troubleshooting

**Port conflict on 5432** — If another PostgreSQL instance is running, change the port mapping in `docker-compose.yml` to `"5433:5432"` and update `DATABASE_URL` accordingly.

**Password authentication failed** — Ensure `POSTGRES_PASSWORD` in `.env` matches the password used when the Docker volume was first created. If mismatched, run `docker compose down -v` to reset and `docker compose up -d postgres` to recreate.

**FEMA API returns 400 Bad Request** — The `$orderby` parameter must reference a valid field in the endpoint. For PublicAssistanceFundedProjectsDetails, use `disasterNumber` not `id`.

**Empty files after editing** — If your editor doesn't save properly, use Python to write files: `python -c "with open('filename', 'w') as f: f.write('content')"`.

---

## Acknowledgments

- [FEMA Open API](https://www.fema.gov/about/openfema/api) for providing open disaster data
- [Faker](https://faker.readthedocs.io/) for realistic synthetic data generation
- [psycopg2](https://www.psycopg.org/) for PostgreSQL connectivity
