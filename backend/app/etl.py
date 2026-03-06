"""ETL pipeline: fetch data from the autochecker API and load it into the database."""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


async def fetch_items() -> list[dict]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        if response.status_code != 200:
            raise Exception(f"Failed to fetch items: {response.status_code} {response.text}")
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    all_logs = []
    params = {"limit": 500}
    if since:
        params["since"] = since.isoformat()

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code} {response.text}")

            data = response.json()
            logs = data["logs"]
            all_logs.extend(logs)

            if not data["has_more"] or not logs:
                break

            # Use last log's submitted_at as the next "since"
            params["since"] = logs[-1]["submitted_at"]

    return all_logs


async def load_items(items: list[dict], session: AsyncSession) -> int:
    new_count = 0
    lab_map = {}  # short ID (e.g. "lab-01") -> ItemRecord

    # Process labs first
    for item in items:
        if item["type"] != "lab":
            continue
        title = item["title"]
        result = await session.exec(
            select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title == title)
        )
        existing = result.first()
        if existing:
            lab_map[item["lab"]] = existing
        else:
            new_item = ItemRecord(type="lab", title=title)
            session.add(new_item)
            await session.flush()  # get the new ID
            lab_map[item["lab"]] = new_item
            new_count += 1

    # Process tasks
    for item in items:
        if item["type"] != "task":
            continue
        parent = lab_map.get(item["lab"])
        if not parent:
            continue
        title = item["title"]
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent.id,
            )
        )
        existing = result.first()
        if not existing:
            new_item = ItemRecord(type="task", title=title, parent_id=parent.id)
            session.add(new_item)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    new_count = 0

    # Build lookup: (lab_short_id, task_short_id_or_None) -> title
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        else:
            title_lookup[(item["lab"], item["task"])] = item["title"]

    for log in logs:
        # 1. Find or create learner
        result = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = result.first()
        if not learner:
            learner = Learner(external_id=log["student_id"], student_group=log["group"])
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        key = (log["lab"], log["task"])
        title = title_lookup.get(key)
        if not title:
            continue
        result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == title)
        )
        item = result.first()
        if not item:
            continue

        # 3. Idempotency check
        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        if result.first():
            continue

        # 4. Create interaction log
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00")).replace(tzinfo=None),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


async def sync(session: AsyncSession) -> dict:
    # Step 1: Fetch and load items
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: Determine last synced timestamp
    result = await session.exec(
        select(InteractionLog.created_at).order_by(InteractionLog.created_at.desc())
    )
    last_timestamp = result.first()

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=last_timestamp)
    new_records = await load_logs(logs, items_catalog, session)

    # Step 4: Count total records
    result = await session.exec(select(InteractionLog))
    total_records = len(result.all())

    return {"new_records": new_records, "total_records": total_records}