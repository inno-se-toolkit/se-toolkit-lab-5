"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
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
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            logs = data["logs"]
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if logs:
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
    from sqlalchemy import select
    from app.models.item import ItemRecord

    new_count = 0

    # Build a mapping from lab short ID (e.g., "lab-01") to ItemRecord
    lab_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item.get("title")
        lab_short_id = item.get("lab")

        # Check if lab already exists by title
        stmt = (
            select(ItemRecord)
            .where(ItemRecord.type == "lab")
            .where(ItemRecord.title == title)
        )
        result = await session.execute(stmt)
        lab_record = result.scalar_one_or_none()

        if lab_record is None:
            # Create new lab record
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            new_count += 1

        # Map lab short ID to the record
        lab_id_to_record[lab_short_id] = lab_record

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        title = item.get("title")
        lab_short_id = item.get("lab")

        # Get parent lab record
        parent_lab = lab_id_to_record.get(lab_short_id)
        if parent_lab is None:
            # Skip task if parent lab not found (shouldn't happen)
            continue

        # Check if task already exists by title and parent_id
        stmt = (
            select(ItemRecord)
            .where(ItemRecord.type == "task")
            .where(ItemRecord.title == title)
            .where(ItemRecord.parent_id == parent_lab.id)
        )
        result = await session.execute(stmt)
        task_record = result.scalar_one_or_none()

        if task_record is None:
            # Create new task record
            task_record = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(task_record)
            new_count += 1

    # Commit all changes
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
    from sqlalchemy import select

    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    new_count = 0

    # Build lookup: (lab_short_id, task_short_id_or_none) -> (title, parent_id)
    # For labs: key = (lab, None), value = (title, None)
    # For tasks: key = (lab, task), value = (title, lab_record.id)
    # We need parent_id to distinguish items with same title (e.g., "Lab setup" in different labs)
    item_lookup: dict[tuple[str, str | None], tuple[str, int | None]] = {}

    # First, load all labs to get their IDs
    stmt = select(ItemRecord).where(ItemRecord.type == "lab")
    result = await session.execute(stmt)
    lab_records = result.scalars().all()
    lab_title_to_id: dict[str, int] = {lab.title: lab.id for lab in lab_records}

    # Build lookup for items
    for item in items_catalog:
        lab_short_id = item.get("lab")
        task_short_id = item.get("task")  # None for labs
        title = item.get("title")

        if task_short_id is None:
            # This is a lab
            key = (lab_short_id, None)
            item_lookup[key] = (title, None)
        else:
            # This is a task - find parent lab ID
            # First, find the lab's title from the catalog
            lab_title = None
            for catalog_item in items_catalog:
                if (
                    catalog_item.get("lab") == lab_short_id
                    and catalog_item.get("type") == "lab"
                ):
                    lab_title = catalog_item.get("title")
                    break

            if lab_title and lab_title in lab_title_to_id:
                parent_id = lab_title_to_id[lab_title]
                key = (lab_short_id, task_short_id)
                item_lookup[key] = (title, parent_id)

    # Process each log
    for log in logs:
        # 1. Find or create Learner
        student_id = log.get("student_id")
        student_group = log.get("group", "")

        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            # Flush to get the learner id
            await session.flush()

        # 2. Find the matching item
        lab_short_id = log.get("lab")
        task_short_id = log.get("task")  # Can be None for lab-level logs

        # Build the lookup key
        lookup_key = (lab_short_id, task_short_id)
        item_info = item_lookup.get(lookup_key)

        if item_info is None:
            # Skip this log if no matching item is found
            continue

        item_title, parent_id = item_info

        # Query DB for ItemRecord with that title (and parent_id if task)
        if parent_id is not None:
            # Task: match by title and parent_id
            stmt = (
                select(ItemRecord)
                .where(ItemRecord.title == item_title)
                .where(ItemRecord.parent_id == parent_id)
            )
        else:
            # Lab: match by title only
            stmt = select(ItemRecord).where(ItemRecord.title == item_title)

        result = await session.execute(stmt)
        item = result.scalar_one_or_none()

        if item is None:
            # Skip this log if no matching item is found
            continue

        # 3. Check if InteractionLog with this external_id already exists
        log_external_id = log.get("id")
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log_external_id
        )
        result = await session.execute(stmt)
        existing_interaction = result.scalar_one_or_none()

        if existing_interaction is not None:
            # Skip if already exists (idempotent)
            continue

        # 4. Create InteractionLog
        from datetime import datetime

        submitted_at_str = log.get("submitted_at")
        submitted_at = (
            datetime.fromisoformat(submitted_at_str) if submitted_at_str else None
        )

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=submitted_at,
        )
        session.add(interaction)
        new_count += 1

    # Commit all changes
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
    from sqlalchemy import select, func

    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(stmt)
    last_synced_at = result.scalar_one()

    # If no records exist, since=None (fetch everything)
    since = last_synced_at

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total count of interactions in DB
    stmt = select(func.count(InteractionLog.id))
    result = await session.execute(stmt)
    total_records = result.scalar_one()

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
