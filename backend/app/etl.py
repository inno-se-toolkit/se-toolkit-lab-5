"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    auth = (settings.autochecker_email, settings.autochecker_password)
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    auth = (settings.autochecker_email, settings.autochecker_password)
    base_url = f"{settings.autochecker_api_url}/api/logs"
    all_logs: list[dict] = []

    current_since = since
    while True:
        params: dict[str, str | int] = {"limit": 500}
        if current_since:
            params["since"] = current_since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(base_url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

        logs = data.get("logs", [])
        all_logs.extend(logs)

        if not data.get("has_more", False):
            break

        if logs:
            last_log = logs[-1]
            submitted_at = last_log.get("submitted_at")
            if submitted_at:
                if isinstance(submitted_at, str):
                    current_since = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
                else:
                    current_since = submitted_at
        else:
            break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0
    lab_short_id_to_record: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    tasks = [item for item in items if item.get("type") == "task"]

    for lab in labs:
        title = lab.get("title", "")
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == title
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing:
            lab_short_id_to_record[lab.get("lab")] = existing
        else:
            new_item = ItemRecord(type="lab", title=title)
            session.add(new_item)
            await session.flush()
            lab_short_id_to_record[lab.get("lab")] = new_item
            new_count += 1

    for task in tasks:
        title = task.get("title", "")
        lab_short_id = task.get("lab")
        parent_lab = lab_short_id_to_record.get(lab_short_id)

        if not parent_lab:
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent_lab.id
        )
        result = await session.exec(stmt)
        existing = result.first()

        if not existing:
            new_item = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(new_item)
            await session.flush()
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    new_count = 0

    lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab")
        task_short_id = item.get("task")
        title = item.get("title", "")
        item_type = item.get("type")

        if item_type == "lab":
            lookup[(lab_short_id, None)] = title
        elif item_type == "task":
            lookup[(lab_short_id, task_short_id)] = title

    for log in logs:
        student_id = log.get("student_id")
        group = log.get("group", "")
        lab_short_id = log.get("lab")
        task_short_id = log.get("task")

        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.exec(stmt)
        learner = result.first()

        if not learner:
            learner = Learner(external_id=student_id, student_group=group)
            session.add(learner)
            await session.flush()

        if task_short_id is not None:
            item_title = lookup.get((lab_short_id, task_short_id))
        else:
            item_title = lookup.get((lab_short_id, None))

        if not item_title:
            continue

        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(stmt)
        item = result.first()

        if not item:
            continue

        log_external_id = log.get("id")
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log_external_id
        )
        result = await session.exec(stmt)
        existing_interaction = result.first()

        if existing_interaction:
            continue

        submitted_at = log.get("submitted_at")
        if isinstance(submitted_at, str):
            created_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
        else:
            created_at = submitted_at or datetime.utcnow()

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    result = await session.exec(stmt)
    last_interaction = result.first()
    since = last_interaction.created_at if last_interaction else None

    raw_logs = await fetch_logs(since=since)
    new_records = await load_logs(raw_logs, raw_items, session)

    stmt = select(InteractionLog)
    result = await session.exec(stmt)
    total_records = len(result.all())

    return {"new_records": new_records, "total_records": total_records}
