"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

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


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch all logs from the autochecker API with pagination."""
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

            current_since = datetime.fromisoformat(logs[-1]["submitted_at"])

    return all_logs


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load labs and tasks into the database.

    Returns the number of newly created records.
    """
    created = 0
    lab_db_by_short_id: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    tasks = [item for item in items if item.get("type") == "task"]

    for lab in labs:
        lab_short_id = lab["lab"]
        lab_title = lab["title"]

        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.attributes["lab"].astext == lab_short_id,
            )
        )
        existing_lab = result.first()

        if existing_lab is None:
            existing_lab = ItemRecord(
                type="lab",
                title=lab_title,
                description=lab.get("description", ""),
                attributes={"lab": lab_short_id},
            )
            session.add(existing_lab)
            await session.flush()
            created += 1

        lab_db_by_short_id[lab_short_id] = existing_lab

    for task in tasks:
        task_title = task["title"]
        task_lab_short_id = task["lab"]
        task_short_id = task["task"]

        parent_lab = lab_db_by_short_id.get(task_lab_short_id)
        if parent_lab is None:
            continue

        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.parent_id == parent_lab.id,
                ItemRecord.attributes["lab"].astext == task_lab_short_id,
                ItemRecord.attributes["task"].astext == task_short_id,
            )
        )
        existing_task = result.first()

        if existing_task is None:
            existing_task = ItemRecord(
                type="task",
                parent_id=parent_lab.id,
                title=task_title,
                description=task.get("description", ""),
                attributes={
                    "lab": task_lab_short_id,
                    "task": task_short_id,
                },
            )
            session.add(existing_task)
            created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Returns the number of newly created interaction records.
    """
    created = 0

    item_map: dict[tuple[str, str | None], dict] = {}
    for item in items_catalog:
        item_map[(item["lab"], item.get("task"))] = item

    for log in logs:
        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == str(log["student_id"]))
        )
        learner = learner_result.first()

        if learner is None:
            learner = Learner(
                external_id=str(log["student_id"]),
                student_group=log.get("group", "") or "",
                enrolled_at=None,
            )
            session.add(learner)
            await session.flush()

        interaction_result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        existing_interaction = interaction_result.first()

        if existing_interaction is not None:
            continue

        catalog_item = item_map.get((log["lab"], log.get("task")))
        if catalog_item is None:
            continue

        item_result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == catalog_item["type"],
                ItemRecord.attributes["lab"].astext == catalog_item["lab"],
                (
                    ItemRecord.attributes["task"].astext == catalog_item["task"]
                    if catalog_item["type"] == "task"
                    else ItemRecord.parent_id.is_(None)
                ),
            )
        )
        item = item_result.first()

        if item is None:
            continue

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        created += 1

    await session.commit()
    return created


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL sync pipeline."""
    items = await fetch_items()
    await load_items(items, session)

    from datetime import timedelta

    result = await session.exec(select(func.max(InteractionLog.created_at)))
    last_synced = result.one()

    if last_synced is not None:
        last_synced = last_synced + timedelta(microseconds=1)

    logs = await fetch_logs(last_synced)
    new_records = await load_logs(logs, items, session)

    total_result = await session.exec(select(func.count()).select_from(InteractionLog))
    total_records = total_result.one()

    return {
        "new_records": new_records,
        "total_records": total_records,
    }