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


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = httpx.BasicAuth(settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)

    response.raise_for_status()
    return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = httpx.BasicAuth(settings.autochecker_email, settings.autochecker_password)

    all_logs: list[dict] = []
    next_since = since.isoformat() if since else None

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if next_since:
                params["since"] = next_since

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()

            payload = response.json()
            page_logs: list[dict] = payload.get("logs", [])
            all_logs.extend(page_logs)

            if not payload.get("has_more", False):
                break

            if not page_logs:
                break

            next_since = str(page_logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    created = 0
    labs_by_short_id: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    tasks = [item for item in items if item.get("type") == "task"]

    for lab in labs:
        lab_title = str(lab["title"])
        lab_short_id = str(lab["lab"])

        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == lab_title
        )
        lab_item = (await session.exec(stmt)).first()

        if lab_item is None:
            lab_item = ItemRecord(type="lab", title=lab_title)
            session.add(lab_item)
            await session.flush()
            created += 1

        labs_by_short_id[lab_short_id] = lab_item

    for task in tasks:
        task_title = str(task["title"])
        parent_lab_short_id = str(task["lab"])
        parent_lab = labs_by_short_id.get(parent_lab_short_id)

        if parent_lab is None:
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == task_title,
            ItemRecord.parent_id == parent_lab.id,
        )
        task_item = (await session.exec(stmt)).first()

        if task_item is None:
            session.add(
                ItemRecord(type="task", title=task_title, parent_id=parent_lab.id)
            )
            created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    created = 0

    title_by_short_ids: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        item_type = str(item.get("type"))
        lab_short_id = str(item.get("lab"))
        task_short_id = item.get("task")

        if item_type == "lab":
            title_by_short_ids[(lab_short_id, None)] = str(item["title"])
        elif item_type == "task":
            title_by_short_ids[(lab_short_id, None if task_short_id is None else str(task_short_id))] = str(
                item["title"]
            )

    learners_cache: dict[str, Learner] = {}
    items_cache: dict[str, ItemRecord | None] = {}
    log_ids_cache: set[int] = set()

    for log in logs:
        student_external_id = str(log["student_id"])
        learner = learners_cache.get(student_external_id)
        if learner is None:
            learner_stmt = select(Learner).where(Learner.external_id == student_external_id)
            learner = (await session.exec(learner_stmt)).first()
            if learner is None:
                learner = Learner(
                    external_id=student_external_id,
                    student_group=str(log.get("group") or ""),
                )
                session.add(learner)
                await session.flush()
            learners_cache[student_external_id] = learner

        log_lab = str(log.get("lab"))
        log_task = log.get("task")
        task_key = None if log_task is None else str(log_task)
        item_title = title_by_short_ids.get((log_lab, task_key))
        if item_title is None:
            continue

        item = items_cache.get(item_title)
        if item_title not in items_cache:
            item_stmt = select(ItemRecord).where(ItemRecord.title == item_title)
            item = (await session.exec(item_stmt)).first()
            items_cache[item_title] = item
        if item is None:
            continue

        external_log_id = int(log["id"])
        if external_log_id in log_ids_cache:
            continue

        existing_stmt = select(InteractionLog).where(
            InteractionLog.external_id == external_log_id
        )
        existing_log = (await session.exec(existing_stmt)).first()
        if existing_log is not None:
            log_ids_cache.add(external_log_id)
            continue

        submitted_at_raw = str(log["submitted_at"])
        submitted_at = datetime.fromisoformat(submitted_at_raw.replace("Z", "+00:00"))
        if submitted_at.tzinfo is not None:
            submitted_at = submitted_at.replace(tzinfo=None)

        session.add(
            InteractionLog(
                external_id=external_log_id,
                learner_id=learner.id,  # type: ignore[arg-type]
                item_id=item.id,  # type: ignore[arg-type]
                kind="attempt",
                score=log.get("score"),
                checks_passed=log.get("passed"),
                checks_total=log.get("total"),
                created_at=submitted_at,
            )
        )
        created += 1
        log_ids_cache.add(external_log_id)

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    items = await fetch_items()
    await load_items(items, session)

    last_synced_stmt = select(func.max(InteractionLog.created_at))
    last_synced_at = (await session.exec(last_synced_stmt)).one()

    logs = await fetch_logs(since=last_synced_at)
    new_records = await load_logs(logs, items, session)

    total_stmt = select(func.count()).select_from(InteractionLog)
    total_records = (await session.exec(total_stmt)).one()

    return {"new_records": int(new_records), "total_records": int(total_records)}
