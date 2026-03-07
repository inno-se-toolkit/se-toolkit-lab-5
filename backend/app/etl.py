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

    TODO: Implement this function.
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
    all_logs: list[dict] = []
    current_since: datetime | None = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Update since to the last log's submitted_at for next page
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(last_log["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Use ItemRecord from app.models.item
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
    from sqlmodel import select
    from app.models.item import ItemRecord

    new_items_count = 0
    lab_id_map: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item["title"]
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == title,
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing is None:
            new_item = ItemRecord(type="lab", title=title)
            session.add(new_item)
            await session.flush()
            lab_id_map[item["lab"]] = new_item
            new_items_count += 1
        else:
            lab_id_map[item["lab"]] = existing

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        title = item["title"]
        lab_short_id = item["lab"]
        parent_lab = lab_id_map.get(lab_short_id)

        if parent_lab is None:
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing is None:
            new_item = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(new_item)
            new_items_count += 1

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
    from datetime import datetime
    from sqlmodel import select
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    # Build lookup: (lab_short_id, task_short_id or None) -> title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short = item["lab"]
        task_short = item.get("task")
        title = item["title"]
        item_title_lookup[(lab_short, task_short)] = title

    new_interactions_count = 0

    for log in logs:
        # 1. Find or create Learner
        student_id = log["student_id"]
        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.exec(stmt)
        learner = result.first()

        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        lab_short = log["lab"]
        task_short = log.get("task")
        item_title = item_title_lookup.get((lab_short, task_short))

        if item_title is None:
            continue

        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(stmt)
        item = result.first()

        if item is None:
            continue

        # 3. Check if InteractionLog already exists
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        result = await session.exec(stmt)
        existing_interaction = result.first()

        if existing_interaction is not None:
            continue

        # 4. Create InteractionLog
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_interactions_count += 1

    await session.commit()
    return new_interactions_count


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
    from sqlmodel import select, func
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Get last synced timestamp
    stmt = select(InteractionLog.created_at).order_by(
        InteractionLog.created_at.desc()
    ).limit(1)
    result = await session.exec(stmt)
    last_interaction = result.first()
    since = last_interaction if last_interaction else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total records count
    stmt = select(func.count(InteractionLog.id))
    result = await session.exec(stmt)
    total_records = result.first() or 0

    return {"new_records": new_records, "total_records": total_records}
