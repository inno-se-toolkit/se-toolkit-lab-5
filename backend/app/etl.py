"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Optional

import httpx
from sqlmodel import select
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

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    auth = httpx.BasicAuth(
        username=settings.autochecker_email,
        password=settings.autochecker_password
    )
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=auth
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
    auth = httpx.BasicAuth(
        username=settings.autochecker_email,
        password=settings.autochecker_password
    )
    
    all_logs = []
    current_since = since
    
    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since.isoformat()
            
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=auth,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            logs = data["logs"]
            all_logs.extend(logs)
            
            if not data.get("has_more") or not logs:
                break
            
            # Use the submitted_at of the last log as the new since value
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
    new_items_count = 0
    lab_by_short_id = {}
    
    # First pass: process labs
    for item in items:
        if item["type"] != "lab":
            continue
            
        # Check if lab already exists
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == item["title"]
        )
        result = await session.execute(statement)
        existing = result.scalar_one_or_none()
        
        if not existing:
            lab = ItemRecord(
                type="lab",
                title=item["title"]
            )
            session.add(lab)
            await session.flush()  # Get the ID without committing
            new_items_count += 1
            lab_by_short_id[item["lab"]] = lab
        else:
            lab_by_short_id[item["lab"]] = existing
    
    # Second pass: process tasks
    for item in items:
        if item["type"] != "task":
            continue
            
        # Find parent lab
        parent_lab = lab_by_short_id.get(item["lab"])
        if not parent_lab:
            # Skip tasks whose parent lab wasn't found
            continue
            
        # Check if task already exists
        statement = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == item["title"],
            ItemRecord.parent_id == parent_lab.id
        )
        result = await session.execute(statement)
        existing = result.scalar_one_or_none()
        
        if not existing:
            task = ItemRecord(
                type="task",
                title=item["title"],
                parent_id=parent_lab.id
            )
            session.add(task)
            await session.flush()
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
    # Build lookup from (lab_short_id, task_short_id) to item title
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        else:  # task
            title_lookup[(item["lab"], item["task"])] = item["title"]
    
    new_logs_count = 0
    
    for log in logs:
        # 1. Find or create Learner
        statement = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.execute(statement)
        learner = result.scalar_one_or_none()
        
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group")
            )
            session.add(learner)
            await session.flush()
        
        # 2. Find matching item
        lookup_key = (log["lab"], log["task"])
        item_title = title_lookup.get(lookup_key)
        
        if not item_title:
            # Skip logs for unknown items
            continue
            
        statement = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.execute(statement)
        item = result.scalar_one_or_none()
        
        if not item:
            # Skip if item not found in DB
            continue
        
        # 3. Check if log already exists (idempotent upsert)
        statement = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        result = await session.execute(statement)
        existing = result.scalar_one_or_none()
        
        if existing:
            continue
        
        # 4. Create new InteractionLog
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"])
        )
        session.add(interaction)
        await session.flush()
        new_logs_count += 1
    
    await session.commit()
    return new_logs_count


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
    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)
    
    # Step 2: Determine last synced timestamp
    statement = select(InteractionLog).order_by(InteractionLog.created_at.desc())
    result = await session.execute(statement)
    last_log = result.first()
    
    since = None
    if last_log:
        since = last_log[0].created_at
    
    # Step 3: Fetch and load logs
    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items, session)
    
    # Get total records count
    statement = select(InteractionLog)
    result = await session.execute(statement)
    total_records = len(result.all())
    
    return {
        "new_records": new_records,
        "total_records": total_records
    }
