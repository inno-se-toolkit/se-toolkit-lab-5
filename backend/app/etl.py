"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
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
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
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
    all_logs: list[dict] = []

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

            if not data.get("has_more", False) or not logs:
                break

            # Use the last log's submitted_at as the new since value
            last_log = logs[-1]
            submitted_at = last_log.get("submitted_at")
            if submitted_at:
                since = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
            else:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
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
    lab_lookup: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Check if lab already exists
        existing = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_item = existing.scalar_one_or_none()

        if lab_item is None:
            lab_item = ItemRecord(type="lab", title=lab_title)
            session.add(lab_item)
            new_count += 1

        # Map short ID to the lab record
        if lab_short_id:
            lab_lookup[lab_short_id] = lab_item

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        task_title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Get parent lab from lookup
        parent_lab = lab_lookup.get(lab_short_id)
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists
        existing = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_item = existing.scalar_one_or_none()

        if task_item is None:
            task_item = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(task_item)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
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

    # Build lookup from (lab_short_id, task_short_id) to item title
    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab", "")
        task_short_id = item.get("task")  # Can be None for labs
        title = item.get("title", "")
        title_lookup[(lab_short_id, task_short_id)] = title

    # Also build a lookup from lab_short_id to lab title (for finding parent)
    lab_title_lookup: dict[str, str] = {}
    for item in items_catalog:
        if item.get("type") == "lab":
            lab_short_id = item.get("lab", "")
            lab_title = item.get("title", "")
            lab_title_lookup[lab_short_id] = lab_title

    new_count = 0

    for log in logs:
        lab_short_id = log.get("lab", "")
        task_short_id = log.get("task")  # Can be None
        student_id = log.get("student_id", "")
        student_group = log.get("group", "unknown")

        # Step 1: Find or create Learner
        learner = await session.execute(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = learner.scalar_one_or_none()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()  # Get learner.id

        # Step 2: Find matching item by title and parent context
        item_title = title_lookup.get((lab_short_id, task_short_id))
        if not item_title:
            # No matching item found, skip this log
            continue

        # For tasks, we need to find the item with the correct parent
        # Get the parent lab title
        parent_lab_title = lab_title_lookup.get(lab_short_id)

        if task_short_id is not None:
            # This is a task - find by title AND parent_id
            # First get the parent lab item
            parent_lab = await session.execute(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == parent_lab_title,
                )
            )
            parent_lab = parent_lab.scalar_one_or_none()

            if parent_lab is None:
                # Parent lab not found, skip
                continue

            # Find the task with this title and parent_id
            item = await session.execute(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == item_title,
                    ItemRecord.parent_id == parent_lab.id,
                )
            )
        else:
            # This is a lab - find by title and type
            item = await session.execute(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == item_title,
                )
            )

        item = item.scalar_one_or_none()

        if item is None:
            # Item not found in DB, skip this log
            continue

        # Step 3: Check if InteractionLog already exists
        existing_interaction = await session.execute(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        if existing_interaction.scalar_one_or_none() is not None:
            # Already exists, skip
            continue

        # Step 4: Create InteractionLog
        submitted_at_str = log.get("submitted_at", "")
        try:
            created_at = datetime.fromisoformat(submitted_at_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            created_at = None

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
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

    # Step 1: Fetch items and load them
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    result = await session.execute(
        select(InteractionLog.created_at)
        .where(InteractionLog.created_at.isnot(None))
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )
    last_synced = result.scalar_one_or_none()

    # Step 3: Fetch logs since last sync and load them
    logs = await fetch_logs(since=last_synced)
    new_records = await load_logs(logs, items, session)

    # Get total records count
    result = await session.execute(select(func.count(InteractionLog.id)))
    total_records = result.scalar()

    return {"new_records": new_records, "total_records": total_records}
