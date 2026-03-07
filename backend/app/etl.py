"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import and_, select
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

    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch items: {response.status_code} {response.text}")

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
    current_since = since
    limit = 500

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": limit}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(url, auth=auth, params=params)

            if response.status_code != 200:
                raise RuntimeError(f"Failed to fetch logs: {response.status_code} {response.text}")

            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the submitted_at of the last log as the new "since" value
            last_log = logs[-1]
            current_since = datetime.fromisoformat(last_log["submitted_at"])

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

    new_items_count = 0

    # Build a mapping from lab_short_id -> ItemRecord for labs
    lab_map: dict[str, ItemRecord] = {}
    # Track lab titles we've seen to avoid duplicate queries
    lab_titles_seen: set[str] = set()

    # Process labs first (items where type="lab")
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_title = item["title"]
        lab_short_id = item["lab"]

        # Skip if we've already processed this lab
        if lab_title in lab_titles_seen:
            continue

        # Check if lab already exists by title
        lab_stmt = select(ItemRecord.id).where(
            and_(ItemRecord.type == "lab", ItemRecord.title == lab_title)
        )
        existing_lab = await session.exec(lab_stmt)
        lab_id = existing_lab.first()

        if lab_id is None:
            # Create new lab record
            lab_record = ItemRecord(type="lab", title=lab_title)
            session.add(lab_record)
            new_items_count += 1
            # Flush immediately to get the ID
            await session.flush()
            lab_id = lab_record.id  # type: ignore[assignment]

        # Get the full record using session.get()
        lab_record = await session.get(ItemRecord, lab_id)

        # Store in map by short_id
        lab_map[lab_short_id] = lab_record  # type: ignore[assignment]
        lab_titles_seen.add(lab_title)

    # Process tasks (items where type="task")
    for item in items:
        if item.get("type") != "task":
            continue

        task_title = item["title"]
        lab_short_id = item["lab"]

        # Get the parent lab record
        parent_lab = lab_map.get(lab_short_id)
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists with this title and parent_id
        task_stmt = select(ItemRecord.id).where(
            and_(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        existing_task = await session.exec(task_stmt)
        task_id = existing_task.first()

        if task_id is None:
            # Create new task record
            task_record = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(task_record)
            new_items_count += 1
        else:
            task_record = await session.get(ItemRecord, task_id)

    # Commit all inserts
    await session.commit()

    return new_items_count


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

    new_interactions_count = 0

    # Build a lookup from (lab_short_id, task_short_id) to item title
    # For labs: key = (lab, None), value = title
    # For tasks: key = (lab, task), value = title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item["lab"]
        task_short_id = item.get("task")  # None for labs
        title = item["title"]
        item_title_lookup[(lab_short_id, task_short_id)] = title

    # Process each log
    for log in logs:
        # Step 1: Find or create Learner by external_id
        student_id = log["student_id"]
        student_group = log.get("group", "")

        learner_stmt = select(Learner).where(Learner.external_id == student_id)
        learner_result = await session.exec(learner_stmt)
        learner = learner_result.scalars().first()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()  # Get the learner.id

        # Step 2: Find the matching item in the database
        lab_short_id = log["lab"]
        task_short_id = log.get("task")  # Can be None for lab-level logs

        # Get the title from our lookup
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            # No matching item found, skip this log
            continue

        # Query the DB for an ItemRecord with that title
        item_stmt = select(ItemRecord.id).where(ItemRecord.title == item_title)
        item_result = await session.exec(item_stmt)
        item_id_row = item_result.first()
        item_id = item_id_row[0] if item_id_row else None

        if item_id is None:
            # No matching item found in DB, skip this log
            continue

        # Step 3: Check if an InteractionLog with this external_id already exists
        log_external_id = log["id"]
        existing_log_stmt = select(InteractionLog.id).where(
            InteractionLog.external_id == log_external_id
        )
        existing_log_result = await session.exec(existing_log_stmt)
        existing_log_id_row = existing_log_result.first()
        existing_log_id = existing_log_id_row[0] if existing_log_id_row else None

        if existing_log_id is not None:
            # Already exists, skip for idempotency
            continue

        # Step 4: Create InteractionLog
        from datetime import datetime

        created_at = datetime.fromisoformat(log["submitted_at"])

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item_id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        new_interactions_count += 1

    # Commit all inserts
    await session.commit()

    return new_interactions_count

#1234567
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
    from sqlalchemy import func, select

    # Step 1: Fetch items from the API and load them
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    last_sync_stmt = select(func.max(InteractionLog.created_at))
    last_sync_result = await session.exec(last_sync_stmt)
    since = last_sync_result.scalar()

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total records count
    total_stmt = select(func.count(InteractionLog.id))
    total_result = await session.exec(total_stmt)
    total_records = total_result.scalar()

    return {"new_records": new_records, "total_records": total_records}
