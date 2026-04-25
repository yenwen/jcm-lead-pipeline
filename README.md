# JCM Lead Pipeline

A Python-based lead pipeline for distressed asset acquisition research, enrichment, underwriting, and review workflows.

## What this repo does

This project is structured as a staged pipeline that ingests candidate leads, normalizes and enriches them, applies distress and underwriting logic, and supports downstream review through a dashboard-oriented workflow.

Current codebase areas include:
- adapters for upstream source collection
- core settings, models, database, and utility logic
- node-based pipeline stages
- tests for core logic and adapter contracts
- a local dashboard entrypoint

## Project structure

- `adapters/` source adapters and collection logic
- `core/` shared models, settings, database, constants, and utilities
- `nodes/` staged pipeline processing logic
- `tests/` unit and contract tests
- `data/` local reference data used by the pipeline
- `dashboard.py` dashboard entrypoint
- `main.py` pipeline entrypoint
- `SPEC.md` working specification

## Setup

### 1. Create a virtual environment

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

Windows PowerShell alternative:
```powershell
Copy-Item .env.example .env
```

Fill in only the keys you actually use. The local `.env` file should never be committed.

### 4. Initialize the database

```bash
python -c "from core.database import init_db; from core.settings import Settings; init_db(Settings().DATABASE_PATH)"
```

### 5. Run tests

```bash
pytest
```

## Environment variables

Common variables currently used by the project:
- `DATABASE_PATH`
- `PROPSTREAM_API_KEY`
- `ATTOM_API_KEY`
- `OPENAI_API_KEY`
- `FIRECRAWL_API_KEY`
- `APIFY_API_TOKEN`
- `AIRTABLE_ACCESS_TOKEN`
- `AIRTABLE_BASE_ID`
- `AIRTABLE_TABLE_NAME`

## Security notes

- Do not commit `.env`, database files, exported lead data, or credentials.
- Treat any live API key as compromised if it is ever committed, even briefly.
- Keep this repo private unless you intentionally want the implementation visible.

## Status

This is an active working repo, not a polished public package. Expect ongoing iteration in pipeline logic, adapters, and underwriting rules.
