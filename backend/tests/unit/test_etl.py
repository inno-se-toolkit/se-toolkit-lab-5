"""Unit tests for the ETL helper functions.

These exercises create a lightweight in-memory database and fake HTTP
responses so the ETL logic can be exercised without a real API server.
"""

import pytest
from datetime import datetime
from sqlalchemy import JSON, event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from app import etl
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# reuse the same engine/session fixtures used in ``test_analytics``


@pytest.fixture
async def engine():
    """An in-memory SQLite engine with the test schema applied."""
    from sqlalchemy.dialects.postgresql import JSONB

    @event.listens_for(SQLModel.metadata, "column_reflect")
    def _reflect(inspector, table, column_info):  # noqa: ANN001 ARG001
        if isinstance(column_info["type"], JSONB):
            column_info["type"] = JSON()

    # also patch the column types on ``ItemRecord`` directly so table
    # creation succeeds under SQLite.
    for col in ItemRecord.__table__.columns:
        if isinstance(col.type, JSONB):
            col.type = JSON()

    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine):
    """Provide a fresh ``AsyncSession`` bound to the test engine."""
    async with AsyncSession(engine) as sess:
        yield sess


# ---------------------------------------------------------------------------
# helper classes for HTTP mocking
# ---------------------------------------------------------------------------


class _DummyResp:
    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class _DummyClient:
    def __init__(self, *args, **kwargs):
        # tests wire the desired behaviour via ``self.next``
        self.next = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, auth=None, params=None):
        # return whatever the test stored in ``next``
        return self.next


# ---------------------------------------------------------------------------
# fetch_* tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_items(monkeypatch):
    dummy = _DummyClient()
    dummy.next = _DummyResp(
        200, [{"lab": "lab-01", "task": None, "title": "Foo", "type": "lab"}]
    )
    monkeypatch.setattr("httpx.AsyncClient", lambda: dummy)

    items = await etl.fetch_items()
    assert isinstance(items, list)
    assert items[0]["title"] == "Foo"


@pytest.mark.asyncio
async def test_fetch_logs_pagination(monkeypatch):
    dummy = _DummyClient()
    # first page contains two logs and has_more True
    page1 = {
        "logs": [{"id": 1, "submitted_at": "2026-01-01T00:00:00Z"}],
        "has_more": True,
    }
    # second page contains one log and has_more False
    page2 = {
        "logs": [{"id": 2, "submitted_at": "2026-01-02T00:00:00Z"}],
        "has_more": False,
    }
    dummy.next = _DummyResp(200, page1)

    # monkeypatch get to alternate responses
    async def _get(url, auth=None, params=None):
        if params and params.get("since") == "2026-01-01T00:00:00Z":
            return _DummyResp(200, page2)
        return _DummyResp(200, page1)

    dummy.get = _get
    monkeypatch.setattr("httpx.AsyncClient", lambda: dummy)

    logs = await etl.fetch_logs()
    assert len(logs) == 2
    # also try with since parameter
    logs2 = await etl.fetch_logs(since=datetime(2026, 1, 1))
    assert len(logs2) == 2


# ---------------------------------------------------------------------------
# database load tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_items_and_idempotency(session):
    items_catalog = [
        {"lab": "lab-01", "task": None, "title": "Lab 1", "type": "lab"},
        {"lab": "lab-01", "task": "setup", "title": "Setup", "type": "task"},
    ]
    new = await etl.load_items(items_catalog, session)
    assert new == 2  # lab + task

    # second run should not create duplicates
    new2 = await etl.load_items(items_catalog, session)
    assert new2 == 0

    # verify they exist in the database
    stmt = ItemRecord.__table__.select()
    result = await session.exec(stmt)
    rows = result.all()
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_load_logs_creates_learner_and_interaction(session):
    # prepare item in DB
    items_catalog = [
        {"lab": "lab-01", "task": None, "title": "Lab 1", "type": "lab"},
        {"lab": "lab-01", "task": "setup", "title": "Setup", "type": "task"},
    ]
    await etl.load_items(items_catalog, session)

    logs = [
        {
            "id": 42,
            "student_id": "stu42",
            "group": "G1",
            "lab": "lab-01",
            "task": "setup",
            "score": 90.0,
            "passed": 3,
            "total": 4,
            "submitted_at": "2026-03-01T12:00:00Z",
        }
    ]
    new = await etl.load_logs(logs, items_catalog, session)
    assert new == 1

    # second insertion should be ignored
    new2 = await etl.load_logs(logs, items_catalog, session)
    assert new2 == 0

    # check learners and interactions
    stmt = Learner.__table__.select()
    learners = (await session.exec(stmt)).all()
    assert len(learners) == 1
    assert learners[0].external_id == "stu42"

    stmt = InteractionLog.__table__.select()
    interactions = (await session.exec(stmt)).all()
    assert len(interactions) == 1
    assert interactions[0].external_id == 42


@pytest.mark.asyncio
async def test_sync_orchestrator(monkeypatch, session):
    # stub fetch_items and fetch_logs to return canned data
    monkeypatch.setattr(
        etl,
        "fetch_items",
        lambda: [{"lab": "lab-01", "task": None, "title": "Lab 1", "type": "lab"}],
    )
    monkeypatch.setattr(
        etl,
        "fetch_logs",
        lambda since=None: [
            {
                "id": 1,
                "student_id": "s1",
                "group": "G",
                "lab": "lab-01",
                "task": None,
                "score": 50,
                "passed": 2,
                "total": 4,
                "submitted_at": "2026-03-01T00:00:00Z",
            }
        ],
    )

    result = await etl.sync(session)
    assert result["new_records"] == 1
    assert result["total_records"] == 1

    # running again should return zero new
    result2 = await etl.sync(session)
    assert result2["new_records"] == 0
    assert result2["total_records"] == 1
