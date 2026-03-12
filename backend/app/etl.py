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
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        timeout=30.0,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    if not isinstance(data, list):
        raise ValueError("Unexpected /api/items response format")

    return data


async def fetch_logs(since: datetime | None = None) -> list[dict]:
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
                params["since"] = _to_api_datetime(current_since)

            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Unexpected /api/logs response format")

            batch = payload.get("logs", [])
            has_more = payload.get("has_more", False)

            if not isinstance(batch, list):
                raise ValueError("Invalid logs payload")

            all_logs.extend(batch)

            if not has_more or not batch:
                break

            current_since = _parse_api_datetime(batch[-1]["submitted_at"])

    return all_logs


async def load_items(items: list[dict], session: AsyncSession) -> int:
    created_count = 0
    lab_map: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    tasks = [item for item in items if item.get("type") == "task"]

    for lab in labs:
        lab_short_id = lab["lab"]
        lab_title = lab["title"]

        existing_lab = (
            await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == lab_title,
                )
            )
        ).first()

        if existing_lab is None:
            existing_lab = ItemRecord(
                type="lab",
                title=lab_title,
            )
            session.add(existing_lab)
            await session.flush()
            created_count += 1

        lab_map[lab_short_id] = existing_lab

    for task in tasks:
        task_title = task["title"]
        parent_lab = lab_map.get(task["lab"])

        if parent_lab is None:
            continue

        existing_task = (
            await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == task_title,
                    ItemRecord.parent_id == parent_lab.id,
                )
            )
        ).first()

        if existing_task is None:
            existing_task = ItemRecord(
                type="task",
                title=task_title,
                parent_id=parent_lab.id,
            )
            session.add(existing_task)
            await session.flush()
            created_count += 1

    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    created_count = 0

    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        title_lookup[(item["lab"], item.get("task"))] = item["title"]

    for log in logs:
        learner = (
            await session.exec(
                select(Learner).where(Learner.external_id == log["student_id"])
            )
        ).first()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        item_title = title_lookup.get((log["lab"], log.get("task")))
        if item_title is None:
            continue

        item = (
            await session.exec(
                select(ItemRecord).where(ItemRecord.title == item_title)
            )
        ).first()

        if item is None:
            continue

        existing_log = (
            await session.exec(
                select(InteractionLog).where(InteractionLog.external_id == log["id"])
            )
        ).first()

        if existing_log is not None:
            continue

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=_parse_api_datetime(log["submitted_at"]),
        )
        session.add(interaction)
        created_count += 1

    await session.commit()
    return created_count


async def sync(session: AsyncSession) -> dict:
    items = await fetch_items()
    await load_items(items, session)

    last_synced_at = (
        await session.exec(select(func.max(InteractionLog.created_at)))
    ).one()

    logs = await fetch_logs(last_synced_at)
    new_records = await load_logs(logs, items, session)

    total_records = (
        await session.exec(select(func.count()).select_from(InteractionLog))
    ).one()

    return {
        "new_records": int(new_records),
        "total_records": int(total_records),
    }


def _parse_api_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _to_api_datetime(value: datetime) -> str:
    return value.isoformat() + "Z"


