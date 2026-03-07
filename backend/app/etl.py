"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import desc, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict[str, Any]]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


async def fetch_logs(since: datetime | None = None) -> list[dict[str, Any]]:
    """Fetch check results from the autochecker API.

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
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict[str, Any]] = []

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if since is not None:
                params["since"] = since.isoformat()

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Update since to the last log's submitted_at for next iteration
            if logs:
                last_log = logs[-1]
                since_str = last_log.get("submitted_at")
                if since_str:
                    since = datetime.fromisoformat(since_str)  # type: ignore[assignment]

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict[str, Any]], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

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
    from app.models.item import ItemRecord

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}  # short_id (e.g. "lab-01") -> ItemRecord

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_short_id = item.get("lab")
        title = item.get("title")

        if not lab_short_id or not title:
            continue

        # Check if lab with this title already exists
        stmt = select(ItemRecord).where(ItemRecord.type == "lab").where(ItemRecord.title == title)
        result = await session.exec(stmt)
        existing = result.first()

        if existing is None:
            # Create new lab record
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            new_count += 1
            # Map short_id to the record (will have id after flush)
            lab_map[lab_short_id] = lab_record

    # Flush to get IDs for lab records
    await session.flush()

    # Populate lab_map for existing labs that weren't just created
    for item in items:
        if item.get("type") != "lab":
            continue
        lab_short_id = item.get("lab")
        title = item.get("title")
        if lab_short_id and title and lab_short_id not in lab_map:
            # Query existing lab by title
            stmt = select(ItemRecord).where(ItemRecord.type == "lab").where(ItemRecord.title == title)
            result = await session.exec(stmt)
            existing_lab = result.first()
            if existing_lab:
                lab_map[lab_short_id] = existing_lab

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        lab_short_id = item.get("lab")
        title = item.get("title")

        if not lab_short_id or not title:
            continue

        # Find parent lab by short_id
        parent_lab = lab_map.get(lab_short_id)
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        parent_id = parent_lab.id

        # Check if task with this title and parent_id already exists
        stmt = (
            select(ItemRecord)
            .where(ItemRecord.type == "task")
            .where(ItemRecord.title == title)
            .where(ItemRecord.parent_id == parent_id)
        )
        result = await session.exec(stmt)
        existing_task = result.first()

        if existing_task is None:
            # Create new task record
            task_record = ItemRecord(type="task", title=title, parent_id=parent_id)
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict[str, Any]], items_catalog: list[dict[str, Any]], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

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
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    # Build lookup: (lab_short_id, task_short_id | None) -> item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab")
        task_short_id = item.get("task")
        title = item.get("title")
        item_type = item.get("type")

        if not lab_short_id or not title:
            continue

        if item_type == "lab":
            key: tuple[str, str | None] = (lab_short_id, None)
            item_title_lookup[key] = title
        elif item_type == "task":
            key = (lab_short_id, task_short_id)
            item_title_lookup[key] = title

    new_count = 0

    for log in logs:
        lab_short_id = log.get("lab")
        task_short_id = log.get("task")
        student_id = log.get("student_id")
        group = log.get("group", "")
        log_id = log.get("id")

        if not lab_short_id or not student_id or log_id is None:
            continue

        # Step 1: Find or create Learner by external_id
        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.exec(stmt)
        learner = result.first()
        if learner is None:
            learner = Learner(external_id=student_id, student_group=group)
            session.add(learner)
            await session.flush()  # Get learner.id

        # Step 2: Find matching item by title
        key: tuple[str, str | None] = (lab_short_id, task_short_id)
        item_title = item_title_lookup.get(key)

        if not item_title:
            # No matching item, skip this log
            continue

        # Query ItemRecord by title
        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(stmt)
        item_record = result.first()

        if item_record is None:
            # No matching item in DB, skip this log
            continue

        # Step 3: Check if InteractionLog with this external_id already exists
        stmt = select(InteractionLog).where(InteractionLog.external_id == log_id)
        result = await session.exec(stmt)
        existing_log = result.first()
        if existing_log is not None:
            # Already exists, skip for idempotency
            continue

        # Step 4: Create new InteractionLog
        submitted_at_str = log.get("submitted_at")
        created_at = (
            datetime.fromisoformat(submitted_at_str) if submitted_at_str else None
        )

        interaction_log = InteractionLog(
            external_id=log_id,
            learner_id=learner.id,  # type: ignore[arg-type]
            item_id=item_record.id,  # type: ignore[arg-type]
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
        )
        if created_at:
            interaction_log.created_at = created_at

        session.add(interaction_log)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline.

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
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine last synced timestamp
    stmt = select(InteractionLog).order_by(desc(InteractionLog.created_at)).limit(1)  # type: ignore[arg-type]
    result = await session.exec(stmt)
    last_log = result.first()
    since = last_log.created_at if last_log else None

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total count of interactions in DB
    total_stmt = select(func.count()).select_from(InteractionLog)
    total_result = await session.exec(total_stmt)
    total_records = total_result.one()

    return {"new_records": new_records, "total_records": total_records}
