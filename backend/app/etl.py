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
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
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
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Update since to the timestamp of the last log
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )
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

    # Build a mapping from lab short ID to lab title
    # Lab short ID is like "lab-01", title is like "Lab 01 — ..."
    lab_title_map: dict[str, str] = {}
    for item in items:
        if item["type"] == "lab":
            lab_title_map[item["lab"]] = item["title"]

    # Process labs first
    lab_records: dict[str, ItemRecord] = {}
    for lab_short_id, lab_title in lab_title_map.items():
        # Check if lab already exists
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title
            )
        )
        existing = result.first()

        if existing is None:
            new_lab = ItemRecord(type="lab", title=lab_title)
            session.add(new_lab)
            await session.flush()  # Get the ID
            lab_records[lab_short_id] = new_lab
            new_count += 1
        else:
            lab_records[lab_short_id] = existing

    # Process tasks
    for item in items:
        if item["type"] != "task":
            continue

        task_title = item["title"]
        lab_short_id = item["lab"]
        lab_record = lab_records.get(lab_short_id)
        parent_id = lab_record.id if lab_record else None

        if parent_id is None:
            continue

        # Check if task already exists
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_id
            )
        )
        existing = result.first()

        if existing is None:
            new_task = ItemRecord(type="task", title=task_title, parent_id=parent_id)
            session.add(new_task)
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

    new_count = 0

    # Build a lookup from (lab, task) to item title
    # For labs: (lab, None) -> title
    # For tasks: (lab, task) -> title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        key = (item["lab"], item.get("task"))
        item_title_lookup[key] = item["title"]

    for log in logs:
        lab_short_id = log["lab"]
        task_short_id = log.get("task")

        # Get the item title from the lookup
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            # Skip if no matching item found
            continue

        # Find or create learner
        student_id = log["student_id"]
        result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = result.first()

        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        # Find the matching item
        result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = result.first()

        if item is None:
            # Skip if no matching item found
            continue

        # Check if interaction already exists (idempotent upsert)
        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        existing_interaction = result.first()

        if existing_interaction is not None:
            continue

        # Create new interaction log
        submitted_at_str = log["submitted_at"]
        submitted_at = datetime.fromisoformat(
            submitted_at_str.replace("Z", "+00:00")
        )

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at,
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

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    result = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc())
    )
    last_interaction = result.first()

    since = last_interaction.created_at if last_interaction else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total records count
    result = await session.exec(
        select(func.count()).select_from(InteractionLog)
    )
    total_records = result.one()

    return {"new_records": new_records, "total_records": total_records}
