"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import desc
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        data = response.json()

    return data


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/logs"
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                url,
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            has_more = data.get("has_more", False)

            if not logs:
                break

            all_logs.extend(logs)

            if not has_more:
                break

            last_submitted_at = logs[-1]["submitted_at"]
            current_since = datetime.fromisoformat(last_submitted_at)

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    from app.models.item import ItemRecord

    created_count = 0
    lab_map: dict[str, ItemRecord] = {}

    labs = [item for item in items if item["type"] == "lab"]
    tasks = [item for item in items if item["type"] == "task"]

    for lab in labs:
        lab_title = lab["title"]

        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == lab_title,
        )
        existing_lab = (await session.exec(statement)).first()

        if existing_lab is None:
            new_lab = ItemRecord(type="lab", title=lab_title)
            session.add(new_lab)
            await session.flush()
            lab_map[lab["lab"]] = new_lab
            created_count += 1
        else:
            lab_map[lab["lab"]] = existing_lab

    for task in tasks:
        task_title = task["title"]
        lab_short_id = task["lab"]
        parent_lab = lab_map.get(lab_short_id)

        if parent_lab is None:
            continue

        statement = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == task_title,
            ItemRecord.parent_id == parent_lab.id,
        )
        existing_task = (await session.exec(statement)).first()

        if existing_task is None:
            new_task = ItemRecord(
                type="task",
                title=task_title,
                parent_id=parent_lab.id,
            )
            session.add(new_task)
            created_count += 1

    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    created_count = 0

    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        key = (item["lab"], item["task"])
        title_lookup[key] = item["title"]

    for log in logs:
        learner_statement = select(Learner).where(
            Learner.external_id == log["student_id"]
        )
        learner = (await session.exec(learner_statement)).first()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log["group"],
            )
            session.add(learner)
            await session.flush()

        item_title = title_lookup.get((log["lab"], log["task"]))
        if item_title is None:
            continue

        item_statement = select(ItemRecord).where(ItemRecord.title == item_title)
        item = (await session.exec(item_statement)).first()

        if item is None:
            continue

        interaction_statement = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        existing_interaction = (await session.exec(interaction_statement)).first()

        if existing_interaction is not None:
            continue

        submitted_at = datetime.fromisoformat(log["submitted_at"])

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at,
        )
        session.add(interaction)
        created_count += 1

    await session.commit()
    return created_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    from app.models.interaction import InteractionLog

    items = await fetch_items()
    await load_items(items, session)

    latest_statement = select(InteractionLog).order_by(desc(InteractionLog.created_at))
    latest_log = (await session.exec(latest_statement)).first()

    since = latest_log.created_at if latest_log is not None else None

    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items, session)

    total_statement = select(InteractionLog)
    total_records = len((await session.exec(total_statement)).all())

    return {
        "new_records": new_records,
        "total_records": total_records,
    }