# backend/app/etl.py
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.settings import settings
from app.models import Item, Learner, InteractionLog  # ← твои модели


# ===================================================================
# 1. fetch_items()
# ===================================================================
async def fetch_items() -> list[dict]:
    """Получаем полный каталог лабораторных и задач."""
    auth = httpx.BasicAuth(
        username=settings.AUTOCHECKER_EMAIL,
        password=settings.AUTOCHECKER_PASSWORD,
    )
    async with httpx.AsyncClient(auth=auth, timeout=30.0) as client:
        url = f"{settings.AUTOCHECKER_API_URL}/api/items"
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


# ===================================================================
# 2. fetch_logs()
# ===================================================================
async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Получаем все логи проверок с пагинацией (has_more)."""
    auth = httpx.BasicAuth(
        username=settings.AUTOCHECKER_EMAIL,
        password=settings.AUTOCHECKER_PASSWORD,
    )

    # Если первый запуск — начинаем с очень старой даты
    current_since = since or datetime(2020, 1, 1, tzinfo=timezone.utc)

    all_logs: list[dict] = []
    limit = 200  # можно увеличить

    async with httpx.AsyncClient(auth=auth, timeout=30.0) as client:
        while True:
            params: dict[str, Any] = {"limit": limit}
            if current_since:
                params["since"] = current_since.isoformat().replace("+00:00", "Z")

            url = f"{settings.AUTOCHECKER_API_URL}/api/logs"
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            batch = data.get("logs", [])
            all_logs.extend(batch)

            if not data.get("has_more", False) or not batch:
                break

            # Берём самый новый timestamp из пачки и сдвигаемся дальше
            last_ts = max(log["submitted_at"] for log in batch)
            current_since = datetime.fromisoformat(
                last_ts.replace("Z", "+00:00")
            ) + timedelta(microseconds=1)

    return all_logs


# ===================================================================
# 3. load_items()
# ===================================================================
async def load_items(session: AsyncSession, items_raw: list[dict]) -> None:
    """Вставляем/обновляем лабораторные и задачи (idempotent)."""
    stmt = pg_insert(Item).values(
        [
            {
                "lab": item["lab"],
                "task": item.get("task"),
                "title": item["title"],
                "type": item["type"],
            }
            for item in items_raw
        ]
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["lab", "task"])
    await session.execute(stmt)
    await session.commit()


# ===================================================================
# 4. load_logs()
# ===================================================================
async def load_logs(
    session: AsyncSession, logs_raw: list[dict], items_raw: list[dict]
) -> None:
    """Создаём/обновляем learners + interaction logs (idempotent)."""
    # Для быстрого поиска item_id по (lab, task)
    item_map = {(it["lab"], it.get("task")): it["title"] for it in items_raw}

    for log in logs_raw:
        # 1. Learner (find or create)
        learner = await session.scalar(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                group=log["group"],
            )
            session.add(learner)
            await session.flush()

        # 2. InteractionLog (idempotent по external_id)
        submitted_at = datetime.fromisoformat(
            log["submitted_at"].replace("Z", "+00:00")
        )

        stmt = pg_insert(InteractionLog).values(
            learner_id=learner.id,
            # item_id можно оставить None или найти, но по требованиям лабы достаточно external_id
            external_id=str(log["id"]),
            score=float(log["score"]),
            checks_passed=log["passed"],
            checks_failed=log["failed"],
            checks_total=log["total"],
            checks=log.get("checks", []),
            submitted_at=submitted_at,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["external_id"])
        await session.execute(stmt)

    await session.commit()


# ===================================================================
# 5. sync() — главная функция пайплайна
# ===================================================================
async def sync(session: AsyncSession) -> dict:
    """Полный цикл ETL: items → logs + статистика."""
    # 1. Items
    items_raw = await fetch_items()
    await load_items(session, items_raw)

    # 2. Последняя дата в БД
    max_ts = await session.scalar(select(func.max(InteractionLog.submitted_at)))

    # 3. Logs (инкрементально)
    logs_raw = await fetch_logs(since=max_ts)

    # 4. Загружаем логи
    await load_logs(session, logs_raw, items_raw)

    # 5. Статистика
    new_records = len(logs_raw)
    total_records = await session.scalar(select(func.count(InteractionLog.id)))

    return {
        "new_records": new_records,
        "total_records": total_records or 0,
    }
