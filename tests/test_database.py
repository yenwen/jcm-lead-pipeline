import pytest
import sqlite3
import os
from core.database import (
    init_db,
    insert_run,
    upsert_lead,
    get_lead,
    list_incomplete_leads,
    get_cache,
    set_cache,
    make_expires_at
)
from core.models import JCMPropertyState

@pytest.fixture
def db_path(tmp_path):
    db_file = tmp_path / "test_jcm.db"
    return str(db_file)

def test_init_db_creates_tables(db_path):
    init_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    assert "runs" in tables
    assert "leads" in tables
    assert "cache" in tables
    conn.close()

def test_insert_run(db_path):
    init_db(db_path=db_path)
    insert_run("run_001", db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT run_id FROM runs")
    assert cursor.fetchone()[0] == "run_001"
    conn.close()

def test_upsert_and_get_lead(db_path):
    init_db(db_path=db_path)
    insert_run("run_001", db_path=db_path)
    state = JCMPropertyState(
        run_id="run_001",
        property_id="prop_001",
        pipeline_stage="new",
        apn="123-456",
        county="Santa Clara"
    )
    upsert_lead(state, db_path=db_path)
    
    fetched = get_lead("run_001", "prop_001", db_path=db_path)
    assert fetched is not None
    assert fetched.run_id == "run_001"
    assert fetched.apn == "123-456"
    
    state.pipeline_stage = "enriched"
    upsert_lead(state, db_path=db_path)
    
    fetched_updated = get_lead("run_001", "prop_001", db_path=db_path)
    assert fetched_updated.pipeline_stage == "enriched"

def test_list_incomplete_leads(db_path):
    init_db(db_path=db_path)
    insert_run("run_1", db_path=db_path)
    
    state1 = JCMPropertyState(run_id="run_1", property_id="p1", pipeline_stage="new")
    state2 = JCMPropertyState(run_id="run_1", property_id="p2", pipeline_stage="published")
    state3 = JCMPropertyState(run_id="run_1", property_id="p3", pipeline_stage="rejected")
    state4 = JCMPropertyState(run_id="run_1", property_id="p4", pipeline_stage="error")
    state5 = JCMPropertyState(run_id="run_1", property_id="p5", pipeline_stage="underwritten")
    
    for state in [state1, state2, state3, state4, state5]:
        upsert_lead(state, db_path=db_path)
        
    incomplete = list_incomplete_leads("run_1", db_path=db_path)
    assert len(incomplete) == 2
    property_ids = {s.property_id for s in incomplete}
    assert property_ids == {"p1", "p5"}

def test_cache_set_get(db_path):
    init_db(db_path=db_path)
    expires = make_expires_at(1)
    set_cache("test_key", "test_value", expires, db_path=db_path)
    
    val = get_cache("test_key", db_path=db_path)
    assert val == "test_value"
    
    assert get_cache("missing_key", db_path=db_path) is None
