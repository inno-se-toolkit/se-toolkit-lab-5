"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    The API returns a JSON array of objects with these keys:
    ``lab``, ``task``, ``title``, ``type``.
    We simply proxy the parsed response.
    """
    # lazily import to keep startup fast
    import httpx

    url = f"{settings.autochecker_api_url.rstrip('/')}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, auth=auth)
        if resp.status_code != 200:
            resp.raise_for_status()
        return resp.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    The endpoint supports pagination via ``limit`` and ``since`` query
    parameters. We loop until ``has_more`` is False, accumulating results.
    ``since`` is expected to be a naive UTC ``datetime``; we convert it to
    an ISO string. When paginating we advance ``since`` to the last
    record's ``submitted_at`` value returned by the API.
    """
    import httpx

    url = f"{settings.autochecker_api_url.rstrip('/')}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    params: dict[str, str | int] = {"limit": 500}
    if since is not None:
        # isoformat without timezone Z, API seems to accept either
        params["since"] = since.isoformat()

    all_logs: list[dict] = []
    async with httpx.AsyncClient() as client:
        while True:
            resp = await client.get(url, auth=auth, params=params)
            if resp.status_code != 200:
                resp.raise_for_status()
            data = resp.json()
            page_logs = data.get("logs", [])
            if not page_logs:
                break
            all_logs.extend(page_logs)
            if data.get("has_more"):
                # advance since to last record's timestamp
                last_ts = page_logs[-1].get("submitted_at")
                if last_ts:
                    params["since"] = last_ts
                # continue looping
                continue
            break
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    The ``items`` list is the raw catalog from ``fetch_items()``. We
    upsert labs first to ensure parent records exist, then create tasks
    referencing the appropriate lab. The function returns the number of
    new rows inserted.
    """
    from app.models.item import ItemRecord
    from sqlmodel import select

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # process labs first
    for entry in items:
        if entry.get("type") != "lab":
            continue
        title = entry.get("title")
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == title
        )
        result = await session.exec(stmt)
        lab_obj = result.first()
        if lab_obj is None:
            lab_obj = ItemRecord(type="lab", title=title)
            session.add(lab_obj)
            await session.flush()
            new_count += 1
        lab_map[entry.get("lab")] = lab_obj

    # now process tasks
    for entry in items:
        if entry.get("type") != "task":
            continue
        title = entry.get("title")
        parent_key = entry.get("lab")
        parent_obj = lab_map.get(parent_key)
        if parent_obj is None:
            # unknown lab, skip
            continue
        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent_obj.id,
        )
        result = await session.exec(stmt)
        task_obj = result.first()
        if task_obj is None:
            task_obj = ItemRecord(type="task", title=title, parent_id=parent_obj.id)
            session.add(task_obj)
            await session.flush()
            new_count += 1
    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    We map the short lab/task identifiers to the title stored in the
    ``item`` table so we can look up the corresponding record. Learners
    are created on-the-fly and interactions are skipped if an entry with
    the same ``external_id`` already exists.
    """
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from sqlmodel import select

    # build lookup of (lab, task) -> title
    lookup: dict[tuple[str, str | None], str] = {}
    for entry in items_catalog:
        key = (entry.get("lab"), entry.get("task"))
        lookup[key] = entry.get("title")

    new_count = 0

    for log in logs:
        student = log.get("student_id")
        group = log.get("group") or ""
        log_id = log.get("id")

        # learner find-or-create
        stmt = select(Learner).where(Learner.external_id == student)
        result = await session.exec(stmt)
        learner = result.first()
        if learner is None:
            learner = Learner(external_id=student, student_group=group)
            session.add(learner)
            await session.flush()

        # map to item title
        title = lookup.get((log.get("lab"), log.get("task")))
        if title is None:
            # no matching item in catalog, skip
            continue
        stmt = select(ItemRecord).where(ItemRecord.title == title)
        result = await session.exec(stmt)
        item_obj = result.first()
        if item_obj is None:
            # catalog entry exists but not created in items table yet
            continue

        # skip existing interaction
        stmt = select(InteractionLog).where(InteractionLog.external_id == log_id)
        result = await session.exec(stmt)
        if result.first() is not None:
            continue

        # create new interaction
        created_at = None
        submitted = log.get("submitted_at")
        if submitted:
            try:
                # handle trailing Z
                created_at = datetime.fromisoformat(submitted.replace("Z", "+00:00"))
            except Exception:
                pass

        interaction = InteractionLog(
            external_id=log_id,
            learner_id=learner.id,
            item_id=item_obj.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at or datetime.utcnow(),
        )
        session.add(interaction)
        new_count += 1
    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    This routine is invoked by the HTTP endpoint. It returns a summary
    of newly added interaction logs and the overall count in the database.
    """
    from app.models.interaction import InteractionLog
    from sqlmodel import select

    # step 1: items
    items = await fetch_items()
    await load_items(items, session)

    # step 2: last timestamp
    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    result = await session.exec(stmt)
    last = result.first()
    since = last.created_at if last else None

    # step 3: logs
    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items, session)

    # total
    stmt = select(InteractionLog)
    all_logs = await session.exec(stmt)
    total = len(all_logs.all())

    return {"new_records": new_records, "total_records": total}
