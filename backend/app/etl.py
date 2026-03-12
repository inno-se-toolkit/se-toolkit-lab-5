"""ETL pipeline: fetch data from the autochecker API and load it into the database."""

import aiohttp
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from app.settings import Settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items(settings: Settings) -> List[Dict[str, Any]]:
    auth = aiohttp.BasicAuth(settings.autochecker_email, settings.autochecker_password)
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{settings.autochecker_api_url}/items", auth=auth) as resp:
            resp.raise_for_status()
            return await resp.json()


async def fetch_logs(settings: Settings, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    auth = aiohttp.BasicAuth(settings.autochecker_email, settings.autochecker_password)
    all_logs = []
    page = 1
    limit = 100
    has_more = True

    async with aiohttp.ClientSession() as session:
        while has_more:
            params = {"page": page, "limit": limit}
            if since:
                params["since"] = since.isoformat()
            async with session.get(f"{settings.autochecker_api_url}/logs", params=params, auth=auth) as resp:
                resp.raise_for_status()
                data = await resp.json()
                logs = data.get("logs", [])
                all_logs.extend(logs)
                has_more = data.get("has_more", False)
                page += 1
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(db: AsyncSession, items_data: List[Dict[str, Any]]) -> None:
    for item in items_data:
        lab = item["lab"]
        task = item.get("task")
        title = item["title"]
        item_type = item["type"]

        stmt = select(ItemRecord).where(
            ItemRecord.lab == lab,
            ItemRecord.task == task,
            ItemRecord.type == item_type
        )
        result = await db.execute(stmt)
        db_item = result.scalar_one_or_none()

        if db_item is None:
            db_item = ItemRecord(
                lab=lab,
                task=task,
                title=title,
                type=item_type,
                description=""
            )
            db.add(db_item)
    await db.commit()


async def load_logs(
    logs: List[dict], items_catalog: List[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    # Build lookup from (lab, task) to title
    lookup = {}
    for item in items_catalog:
        lab = item["lab"]
        task = item.get("task")
        title = item["title"]
        lookup[(lab, task)] = title

    new_count = 0
    for log in logs:
        # 1. Find or create Learner
        student_id = log["student_id"]
        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()
        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=log.get("group", "unknown")
            )
            session.add(learner)
            await session.flush()

        # 2. Find matching ItemRecord by (lab, task)
        key = (log["lab"], log["task"])
        title = lookup.get(key)
        if title is None:
            continue
        stmt = select(ItemRecord).where(ItemRecord.title == title)
        result = await session.execute(stmt)
        item_record = result.scalar_one_or_none()
        if item_record is None:
            continue

        # 3. Check idempotency: skip if InteractionLog with this external_id exists
        stmt = select(InteractionLog).where(InteractionLog.external_id == log["id"])
        result = await session.execute(stmt)
        if result.scalar_one_or_none() is not None:
            continue

        # 4. Create InteractionLog
        submitted_at = datetime.fromisoformat(log["submitted_at"].replace('Z', '+00:00'))
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item_record.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at
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
    settings = Settings()

    # Step 1: Fetch items and load them
    items_data = await fetch_items(settings)
    await load_items(session, items_data)

    # Step 2: Determine last sync timestamp
    result = await session.execute(select(func.max(InteractionLog.created_at)))
    last_created = result.scalar_one()
    since = last_created if last_created is not None else None

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(settings, since=since)
    new_records = await load_logs(logs, items_data, session)

    # Step 4: Count total records in DB
    result = await session.execute(select(func.count()).select_from(InteractionLog))
    total_records = result.scalar_one()

    return {"new_records": new_records, "total_records": total_records}
