"""ETL pipeline for fetching data from autochecker API."""

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from app import models
from app.settings import settings
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

AUTH = (settings.AUTOCHEKER_EMAIL, settings.AUTOCHEKER_PASSWORD)


async def fetch_items() -> List[Dict[str, Any]]:
    """Fetch lab/task catalog from autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://auche.namaz.live/api/items",
            auth=AUTH
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> List[Dict[str, Any]]:
    """Fetch check logs with pagination."""
    all_logs = []
    url = "https://auche.namaz.live/api/logs"
    params = {"limit": 100}
    if since:
        params["since"] = since.isoformat().replace("+00:00", "Z")

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(url, params=params, auth=AUTH)
            response.raise_for_status()
            data = response.json()
            all_logs.extend(data["logs"])
            if not data.get("has_more"):
                break
            params["page"] = data.get("next_page", 1)
    return all_logs


async def load_items(session: AsyncSession, items_data: List[Dict[str, Any]]) -> Dict[str, models.Item]:
    """Insert items into database, return mapping of (lab, task) -> Item."""
    mapping = {}
    for item in items_data:
        db_item = models.Item(
            external_id=f"{item['lab']}_{item['task']}" if item['task'] else item['lab'],
            type="task" if item['task'] else "lab",
            title=item['title']
        )
        session.add(db_item)
        await session.flush()
        key = (item['lab'], item['task'])
        mapping[key] = db_item
    await session.commit()
    return mapping


async def load_logs(session: AsyncSession, logs_data: List[Dict[str, Any]], item_map: Dict[Tuple[str, str], models.Item]) -> int:
    """Insert logs into database with learner creation."""
    new_count = 0
    for log in logs_data:
        # Find or create learner
        stmt = select(models.Learner).where(models.Learner.external_id == log["student_id"])
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()
        if not learner:
            learner = models.Learner(
                external_id=log["student_id"],
                group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()

        # Find item
        item_key = (log["lab"], log.get("task"))
        item = item_map.get(item_key)
        if not item:
            logger.warning(f"Item not found for {item_key}, skipping")
            continue

        # Check if interaction already exists (idempotency)
        stmt = select(models.InteractionLog).where(
            models.InteractionLog.external_id == str(log["id"])
        )
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            continue

        # Create interaction
        interaction = models.InteractionLog(
            external_id=str(log["id"]),
            learner_id=learner.id,
            item_id=item.id,
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            submitted_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00"))
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


async def sync(session: AsyncSession) -> Dict[str, int]:
    """Run full ETL pipeline."""
    # Step 1: Fetch items
    items_data = await fetch_items()
    item_map = await load_items(session, items_data)

    # Step 2: Find latest log timestamp
    stmt = select(models.InteractionLog).order_by(models.InteractionLog.submitted_at.desc()).limit(1)
    result = await session.execute(stmt)
    latest = result.scalar_one_or_none()
    since = latest.submitted_at if latest else None

    # Step 3: Fetch and load logs
    logs_data = await fetch_logs(since)
    new_records = await load_logs(session, logs_data, item_map)

    # Step 4: Count total
    stmt = select(models.InteractionLog)
    result = await session.execute(stmt)
    total = len(result.scalars().all())

    return {"new_records": new_records, "total_records": total}
