"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items  lab/task catalog
- GET /api/logs   anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract  fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        timeout=30.0,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    url = f"{settings.autochecker_api_url}/api/logs"
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        timeout=60.0,
    ) as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            batch_logs = data.get("logs", [])
            has_more = data.get("has_more", False)

            if not batch_logs:
                break

            all_logs.extend(batch_logs)

            if not has_more:
                break

            last_submitted_at = batch_logs[-1]["submitted_at"]
            current_since = datetime.fromisoformat(last_submitted_at)

    return all_logs


# ---------------------------------------------------------------------------
# Load  insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    created_count = 0
    labs_by_short_id: dict[str, ItemRecord] = {}

    labs = [item for item in items if item["type"] == "lab"]
    tasks = [item for item in items if item["type"] == "task"]

    # Process labs first
    for lab in labs:
        lab_title = lab["title"]
        lab_short_id = lab["lab"]

        existing_lab = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_record = existing_lab.first()

        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=lab_title)
            session.add(lab_record)
            await session.flush()
            created_count += 1

        labs_by_short_id[lab_short_id] = lab_record

    # Process tasks
    for task in tasks:
        task_title = task["title"]
        lab_short_id = task["lab"]
        parent_lab = labs_by_short_id.get(lab_short_id)

        if parent_lab is None:
            continue

        existing_task = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing_task.first()

        if task_record is None:
            task_record = ItemRecord(
                type="task",
                title=task_title,
                parent_id=parent_lab.id,
            )
            session.add(task_record)
            await session.flush()
            created_count += 1

    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    created_count = 0

    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        key = (item["lab"], item["task"])
        item_title_lookup[key] = item["title"]

    for log in logs:
        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = learner_result.first()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", "") or "",
            )
            session.add(learner)
            await session.flush()

        item_title = item_title_lookup.get((log["lab"], log["task"]))
        if item_title is None:
            continue

        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()
        if item is None:
            continue

        existing_interaction_result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        existing_interaction = existing_interaction_result.first()

        if existing_interaction is not None:
            continue

        created_at = datetime.fromisoformat(log["submitted_at"])

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
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
    items = await fetch_items()
    await load_items(items, session)

    last_synced_result = await session.exec(select(func.max(InteractionLog.created_at)))
    last_synced_at = last_synced_result.one()

    logs = await fetch_logs(since=last_synced_at)
    new_records = await load_logs(logs, items, session)

    total_records_result = await session.exec(select(func.count(InteractionLog.id)))
    total_records = total_records_result.one()

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
