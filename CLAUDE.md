# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**GeoObras** is a backend for monitoring public works (obras públicas) in Macaé/RJ, Brazil. It ingests data from external APIs (ObrasGov.br and TCE-RJ) and CSV files, processes them through an ETL pipeline, and exposes a REST API.

## Commands

### Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/Scripts/activate   # Windows bash
# or: .venv\Scripts\Activate.ps1  (PowerShell)

pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
```

### Database

```bash
# Apply the DDL (requires a running Postgres instance)
psql -U geoobras -d geoobras -f sql/001_create_schemas.sql
```

### Run the API

```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

### Run the ETL

```bash
# Full pipeline (RAW → CLEAN → Analytics)
python -m src.etl.run_etl

# Single data source only
python -m src.etl.run_etl --fonte obrasgov
python -m src.etl.run_etl --fonte tcerj
python -m src.etl.run_etl --fonte convenios

# Skip RAW ingestion, reprocess CLEAN + Analytics only
python -m src.etl.run_etl --skip-raw
```

### Docker

```bash
docker build -t geoobras-backend .
docker run -p 8000:8000 --env-file .env geoobras-backend
```

## Architecture

### Three-Layer Database Schema

The PostgreSQL database uses three schemas that mirror the ETL stages:

- **`raw`** – mirrors of external API responses, stored as-is (JSONB payloads preserved for reprocessing). Tables: `obrasgov_projetos`, `obrasgov_execucao_fisica`, `obrasgov_execucao_financeira`, `obrasgov_contratos`, `obrasgov_geometria`, `tcerj_obras`, `tcerj_obras_paralisadas`, `macae_convenios`.
- **`clean`** – unified, normalized model filtered to Macaé. Central table: `clean.obras` (UUID PK `id_obra_geoobras`). Also: `clean.contratos`, `clean.obras_contratos` (N:M junction), `clean.convenios`.
- **`analytics`** – derived metrics: `analytics.metricas_obra` (financial/physical progress, delay flags), `analytics.recorrencia_territorial` (spatial base, expanded in later iterations).
- **`etl_execucao`** (public schema) – audit log of every ETL run.

### ETL Pipeline (`src/etl/run_etl.py`)

Three sequential phases:
1. **RAW ingestion** (`ingestion_service`) – fetches paginated data from ObrasGov API (all of RJ state; Macaé filtering happens in CLEAN) and TCE-RJ API, plus reads CSV files from `CONVENIOS_DIR`.
2. **CLEAN normalization** (`clean_service`) – filters for Macaé, normalizes field names and dates, applies heuristic name-similarity matching (threshold 0.60 via `SequenceMatcher`) to deduplicate ObrasGov ↔ TCE-RJ obras, generates quality flags.
3. **Analytics** (`analytics_service`) – computes `percentual_desembolso`, `dias_atraso`, and `flag_possivel_atraso` from `clean.obras`.

### API (`src/api/main.py`)

FastAPI app at `src/api/main:app`. Endpoints:
- `GET /health` – liveness check
- `GET /api/v1/obras` – paginated list with filters (`situacao`, `municipio`, `apenas_com_coordenadas`, `apenas_inconsistencias`, `valor_minimo`)
- `GET /api/v1/obras/{id}` – full detail (UUID)
- `GET /api/v1/estatisticas` – aggregate stats
- `POST /api/v1/refresh` – logs ETL intent; actual execution is via cron (not synchronous)

### Key Design Decisions

- **No ORM models** – the project uses SQLAlchemy Core (raw SQL / `text()` queries) in repositories; `src/domain/models.py` contains Pydantic v2 models for data-in-transit, not ORM mappings.
- **Geometry without PostGIS** – geometries are stored as WKT `TEXT` in `geom` columns. `shapely` is used to extract lat/lon from WKT. PostGIS substitution points are marked with `[POSTGIS]` comments in the SQL DDL.
- **Settings via `pydantic-settings`** – all configuration in `src/config/settings.py`, loaded from `.env`. Access via `get_settings()` (cached with `lru_cache`).
- **Municipio filtering heuristic** – ObrasGov API returns the entire state of RJ (no municipality filter). Obras are matched to Macaé by substring search across `endereco`, `nome`, `descricao`, and `municipio` fields in the CLEAN phase.
- **HTTP clients** – `src/infra/http_clients/obrasgov_client.py` and `tcerj_client.py` use `httpx` with retry logic. Settings `HTTP_TIMEOUT` and `HTTP_MAX_RETRIES` control behavior.
- **Convênios CSV** – files must be placed in `CONVENIOS_DIR` (default: `data/input/macae_convenios/`). Encoding is assumed `latin-1` (common for Brazilian government files).
