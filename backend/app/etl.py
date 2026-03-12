"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.
    
    Returns:
        list[dict]: List of items from the API
        
    Raises:
        Exception: If the API request fails or returns non-200 status
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = httpx.BasicAuth(
        username=settings.autochecker_email,
        password=settings.autochecker_password
    )
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        
        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch items: {response.status_code} - {response.text}"
            )
        
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination.
    
    Args:
        since: Optional timestamp to fetch logs after this time
        
    Returns:
        list[dict]: Combined list of all logs from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = httpx.BasicAuth(
        username=settings.autochecker_email,
        password=settings.autochecker_password
    )
    
    all_logs = []
    current_since = since
    has_more = True
    
    async with httpx.AsyncClient() as client:
        while has_more:
            # Prepare query parameters
            params = {"limit": 500}
            if current_since:
                # Convert datetime to ISO format string
                params["since"] = current_since.isoformat()
            
            # Make the request
            response = await client.get(url, auth=auth, params=params)
            
            if response.status_code != 200:
                raise Exception(
                    f"Failed to fetch logs: {response.status_code} - {response.text}"
                )
            
            data = response.json()
            
            # Add logs from this page
            all_logs.extend(data["logs"])
            
            # Check if there are more pages
            has_more = data.get("has_more", False)
            
            if has_more and data["logs"]:
                # Use the submitted_at of the last log as the next since value
                last_log = data["logs"][-1]
                # Parse ISO format string to datetime (handle Z suffix)
                last_log_time = last_log["submitted_at"].replace('Z', '+00:00')
                current_since = datetime.fromisoformat(last_log_time)
    
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.
    
    Args:
        items: List of items from the API
        session: Database session
        
    Returns:
        int: Number of newly created items
    """
    new_count = 0
    
    # Dictionary to map lab short_id -> lab database record
    lab_records = {}
    
    # First process labs (type="lab")
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
            # Create new lab record
            lab = ItemRecord(
                type="lab",
                title=item["title"],
                description="",
                attributes={}
            )
            session.add(lab)
            await session.flush()  # Get the ID
            new_count += 1
            existing = lab
        
        # Store in lookup dictionary by short_id
        lab_records[item["lab"]] = existing
    
    # Then process tasks (type="task")
    for item in items:
        if item["type"] != "task":
            continue
        
        # Find parent lab
        parent_lab = lab_records.get(item["lab"])
        if not parent_lab:
            print(f"Warning: Parent lab not found for task {item['title']}")
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
            # Create new task record
            task = ItemRecord(
                type="task",
                title=item["title"],
                description="",
                parent_id=parent_lab.id,
                attributes={}
            )
            session.add(task)
            new_count += 1
    
    # Commit all changes
    await session.commit()
    
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.
    
    Args:
        logs: Raw log dicts from the API
        items_catalog: Raw item dicts from fetch_items() for mapping
        session: Database session
        
    Returns:
        int: Number of newly created interactions
    """
    new_count = 0
    
    # Build lookup dictionary: (lab_short_id, task_short_id) -> title
    # For labs, key is (lab, None). For tasks, key is (lab, task)
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        else:  # task
            title_lookup[(item["lab"], item["task"])] = item["title"]
    
    # Cache for learners to avoid repeated DB queries
    learner_cache = {}
    
    # Process each log
    for log in logs:
        # 1. Find or create Learner
        external_id = log["student_id"]
        if external_id not in learner_cache:
            # Check if learner exists
            statement = select(Learner).where(Learner.external_id == external_id)
            result = await session.execute(statement)
            learner = result.scalar_one_or_none()
            
            if not learner:
                # Create new learner
                learner = Learner(
                    external_id=external_id,
                    student_group=log.get("group", "")
                )
                session.add(learner)
                await session.flush()  # Get the ID
            
            learner_cache[external_id] = learner
        else:
            learner = learner_cache[external_id]
        
        # 2. Find matching Item
        lab_short = log["lab"]
        task_short = log.get("task")  # May be None for lab-level logs
        title = title_lookup.get((lab_short, task_short))
        
        if not title:
            print(f"Warning: No matching item for lab={lab_short}, task={task_short}")
            continue
        
        # Find item in database by title AND parent relationship
        if task_short is None:
            # This is a lab-level log - find by title and type
            statement = select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == title
            )
        else:
            # This is a task-level log - need to find by title and parent lab
            # First find the parent lab title
            lab_title = title_lookup.get((lab_short, None))
            if not lab_title:
                print(f"Warning: No lab title found for {lab_short}")
                continue
                
            # Find parent lab in database
            lab_statement = select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title
            )
            lab_result = await session.execute(lab_statement)
            parent_lab = lab_result.scalar_one_or_none()
            
            if not parent_lab:
                print(f"Warning: Parent lab not found for {title}")
                continue
            
            # Find task with this title and parent_id
            statement = select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_lab.id
            )
        
        result = await session.execute(statement)
        item = result.scalar_one_or_none()
        
        if not item:
            print(f"Warning: Item with title '{title}' not found in database")
            continue
        
        # 3. Check if log already exists (idempotency)
        log_external_id = log["id"]
        statement = select(InteractionLog).where(
            InteractionLog.external_id == log_external_id
        )
        result = await session.execute(statement)
        existing_log = result.scalar_one_or_none()
        
        if existing_log:
            # Skip if already exists
            continue
        
        # 4. Create new InteractionLog
        # Parse submitted_at timestamp
        submitted_at_str = log["submitted_at"].replace('Z', '+00:00')
        submitted_at = datetime.fromisoformat(submitted_at_str)
        
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",  # All logs are attempts
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=submitted_at
        )
        session.add(interaction)
        new_count += 1
        
        # Periodic flush to avoid memory issues
        if new_count % 100 == 0:
            await session.flush()
    
    # Final commit
    await session.commit()
    
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.
    
    Returns:
        dict: {"new_records": number of new interactions,
               "total_records": total interactions in database}
    """
    # Step 1: Fetch and load items
    print("Fetching items from API...")
    items_catalog = await fetch_items()
    print(f"Fetched {len(items_catalog)} items")
    
    print("Loading items into database...")
    new_items = await load_items(items_catalog, session)
    print(f"Loaded {new_items} new items")
    
    # Step 2: Determine last sync timestamp
    print("Checking last sync time...")
    statement = select(func.max(InteractionLog.created_at))
    result = await session.execute(statement)
    last_sync = result.scalar_one()
    
    if last_sync:
        print(f"Last sync: {last_sync}")
        since = last_sync
    else:
        print("No previous sync, fetching all logs")
        since = None
    
    # Step 3: Fetch and load logs
    print("Fetching logs from API...")
    logs = await fetch_logs(since=since)
    print(f"Fetched {len(logs)} logs")
    
    if logs:
        print("Loading logs into database...")
        new_interactions = await load_logs(logs, items_catalog, session)
        print(f"Loaded {new_interactions} new interactions")
    else:
        new_interactions = 0
        print("No new logs to load")
    
    # Get total interactions count
    statement = select(func.count(InteractionLog.id))
    result = await session.execute(statement)
    total_interactions = result.scalar_one()
    
    return {
        "new_records": new_interactions,
        "total_records": total_interactions
    }
