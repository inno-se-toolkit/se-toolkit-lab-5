"""ETL pipeline: fetch data from the autochecker API and load it into the database.
"""

from datetime import datetime
import httpx

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )

        response.raise_for_status()

        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    logs: list[dict] = []
    params = {"limit": 500}

    if since:
        params["since"] = since.isoformat()

    async with httpx.AsyncClient() as client:

        while True:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )

            response.raise_for_status()
            data = response.json()

            batch = data["logs"]
            logs.extend(batch)

            if not data["has_more"] or not batch:
                break

            last_timestamp = batch[-1]["submitted_at"]
            params["since"] = last_timestamp

    return logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    created = 0
    lab_map: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item["type"] != "lab":
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == item["title"],
        )

        result = await session.exec(stmt)
        lab = result.first()

        if not lab:
            lab = ItemRecord(
                type="lab",
                title=item["title"],
            )
            session.add(lab)
            await session.flush()
            created += 1

        lab_map[item["lab"]] = lab

    # Process tasks
    for item in items:
        if item["type"] != "task":
            continue

        parent_lab = lab_map.get(item["lab"])
        if not parent_lab:
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == item["title"],
            ItemRecord.parent_id == parent_lab.id,
        )

        result = await session.exec(stmt)
        existing = result.first()

        if not existing:
            task = ItemRecord(
                type="task",
                title=item["title"],
                parent_id=parent_lab.id,
            )
            session.add(task)
            created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:

    created = 0

    # Build lookup: (lab, task) -> title
    lookup: dict[tuple[str, str | None], str] = {}

    for item in items_catalog:
        key = (item["lab"], item["task"])
        lookup[key] = item["title"]

    for log in logs:

        # Find or create learner
        stmt = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.exec(stmt)
        learner = result.first()

        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log["group"],
            )
            session.add(learner)
            await session.flush()

        # Find item title
        title = lookup.get((log["lab"], log["task"]))
        if not title:
            continue

        stmt = select(ItemRecord).where(ItemRecord.title == title)
        result = await session.exec(stmt)
        item = result.first()

        if not item:
            continue

        # Check idempotency
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        result = await session.exec(stmt)

        if result.first():
            continue

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00")),
        )

        session.add(interaction)
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:

    # Fetch items
    items = await fetch_items()

    await load_items(items, session)

    # Determine last synced timestamp
    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc())
    result = await session.exec(stmt)
    last_log = result.first()

    since = last_log.created_at if last_log else None

    # Fetch logs
    logs = await fetch_logs(since)

    new_records = await load_logs(logs, items, session)

    # Total records
    stmt = select(InteractionLog)
    result = await session.exec(stmt)
    total_records = len(result.all())

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
