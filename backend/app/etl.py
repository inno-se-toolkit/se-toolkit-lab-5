"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import desc
from sqlalchemy.sql import column
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict] = []

    async with httpx.AsyncClient() as client:
        current_since = since

        while True:
            params: dict = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if not logs:
                break

            last_log = logs[-1]
            submitted_at = last_log.get("submitted_at")
            if not submitted_at:
                break

            current_since = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    from app.models.item import ItemRecord

    new_count = 0
    lab_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_title = item.get("title") or ""
        lab_short_id = item.get("lab") or ""

        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_record = existing.one_or_none()

        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=lab_title)
            session.add(lab_record)
            await session.flush()
            new_count += 1

        if lab_short_id:
            lab_id_to_record[lab_short_id] = lab_record

    # Process tasks after labs
    for item in items:
        if item.get("type") != "task":
            continue

        task_title = item.get("title") or ""
        lab_short_id = item.get("lab") or ""

        parent_lab = lab_id_to_record.get(lab_short_id)
        if parent_lab is None:
            continue

        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing.one_or_none()

        if task_record is None:
            task_record = ItemRecord(
                type="task",
                title=task_title,
                parent_id=parent_lab.id,
            )
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    new_count = 0

    item_lookup: dict[tuple[str, str | None], dict] = {}
    for item in items_catalog:
        key = (item.get("lab") or "", item.get("task"))
        item_lookup[key] = item

    for log in logs:
        student_id = log.get("student_id") or ""
        student_group = log.get("group") or ""

        learner = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner_record = learner.one_or_none()

        if learner_record is None:
            learner_record = Learner(
                external_id=student_id,
                student_group=student_group,
            )
            session.add(learner_record)
            await session.flush()

        lab_short_id = log.get("lab") or ""
        task_short_id = log.get("task")
        catalog_item = item_lookup.get((lab_short_id, task_short_id))

        if catalog_item is None:
            continue

        item_record = None

        if catalog_item.get("type") == "lab":
            lab_title = catalog_item.get("title") or ""
            item_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == lab_title,
                )
            )
            item_record = item_result.one_or_none()

        elif catalog_item.get("type") == "task":
            lab_catalog_item = item_lookup.get((lab_short_id, None))
            if lab_catalog_item is None:
                continue

            lab_title = lab_catalog_item.get("title") or ""
            lab_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == lab_title,
                )
            )
            lab_record = lab_result.one_or_none()
            if lab_record is None:
                continue

            task_title = catalog_item.get("title") or ""
            task_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == task_title,
                    ItemRecord.parent_id == lab_record.id,
                )
            )
            item_record = task_result.one_or_none()

        if item_record is None:
            continue

        log_external_id = log.get("id")
        existing_interaction = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        )
        if existing_interaction.one_or_none() is not None:
            continue

        submitted_at_str = log.get("submitted_at")
        created_at = None
        if submitted_at_str:
            created_at = datetime.fromisoformat(submitted_at_str.replace("Z", "+00:00"))

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner_record.id,
            item_id=item_record.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
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
    from app.models.interaction import InteractionLog

    items = await fetch_items()
    await load_items(items, session)

    latest = await session.exec(
        select(InteractionLog.created_at)
        .where(column("created_at").isnot(None))
        .order_by(desc(column("created_at")))
        .limit(1)
    )
    latest_record = latest.one_or_none()
    since = latest_record if latest_record else None

    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
