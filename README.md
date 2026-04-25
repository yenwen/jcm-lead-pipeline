# JCM Multimodal Distressed Asset Acquisition Pipeline

Phase 1 scaffold for Sprint 1.

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Environment variables:
   ```bash
   cp .env.example .env
   ```
4. Initialize the database:
   ```bash
   python -c "from core.database import init_db; from core.settings import Settings; init_db(Settings().DATABASE_PATH)"
   ```
5. Run tests:
   ```bash
   pytest
   ```

## Note
Nodes 1-9 are not implemented yet in this Sprint 1 foundation.
