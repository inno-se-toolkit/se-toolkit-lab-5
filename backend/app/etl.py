"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings

from sqlmodel import select
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from sqlmodel import select
from sqlmodel import func, select
# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


# backend/app/etl.py
import httpx
from app.settings import settings

async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password)
        )
        response.raise_for_status()  
        return response.json()




async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    all_logs = []
    current_since = since
    base_url = f"{settings.autochecker_api_url}/api/logs"
    
    async with httpx.AsyncClient() as client:
        while True:
            
            params = {"limit": 500}
            if current_since:
              
                params["since"] = current_since.isoformat()
            
            
            response = await client.get(
                base_url,
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            
            logs = data.get("logs", [])
            all_logs.extend(logs)
            
            
            if not data.get("has_more", False) or not logs:
                break
                
            
            last_log = logs[-1]
            current_since = datetime.fromisoformat(last_log["submitted_at"].replace('Z', '+00:00'))
    
    return all_logs

# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    created_count = 0
    lab_by_short_id = {}  # {"lab-01": ItemRecord, ...}
    
    
    for item in items:
        if item["type"] == "lab":
            
            stmt = select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == item["title"]
            )
            result = await session.execute(stmt)
            lab_record = result.scalar_one_or_none()
            
            if not lab_record:
               
                lab_record = ItemRecord(
                    type="lab",
                    title=item["title"]
                )
                session.add(lab_record)
                await session.flush()  
                created_count += 1
            
            
            lab_by_short_id[item["lab"]] = lab_record
    
    
    for item in items:
        if item["type"] == "task":
            
            parent_lab = lab_by_short_id.get(item["lab"])
            if not parent_lab:
                print(f"Предупреждение: лаба {item['lab']} не найдена для таска {item['title']}")
                continue
            
            
            stmt = select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == item["title"],
                ItemRecord.parent_id == parent_lab.id
            )
            result = await session.execute(stmt)
            task_record = result.scalar_one_or_none()
            
            if not task_record:
                
                task_record = ItemRecord(
                    type="task",
                    title=item["title"],
                    parent_id=parent_lab.id
                )
                session.add(task_record)
                created_count += 1
    
    
    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    created_count = 0
    
    
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        else:  
            title_lookup[(item["lab"], item["task"])] = item["title"]
    
    
    stmt = select(InteractionLog.external_id)
    result = await session.execute(stmt)
    existing_ids = {row[0] for row in result}
    
    
    for log in logs:
        
        if log["id"] in existing_ids:
            continue
        
        
        stmt = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()
        
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()
        
        
        key = (log["lab"], log.get("task"))
        item_title = title_lookup.get(key)
        
        if not item_title:
            print(f"Предупреждение: item не найден для {key}")
            continue
            
        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.execute(stmt)
        item = result.scalar_one_or_none()
        
        if not item:
            print(f"Предупреждение: item {item_title} не найден в БД")
            continue
        
        
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score", 0),
            checks_passed=log.get("passed", 0),
            checks_total=log.get("total", 0),
            created_at=datetime.fromisoformat(log["submitted_at"].replace('Z', '+00:00'))
        )
        session.add(interaction)
        created_count += 1
        
        
        existing_ids.add(log["id"])
    
    await session.commit()
    return created_count

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def sync(session: AsyncSession) -> dict:
    """
    Run the full ETL pipeline.
    
    This is the main entry point that coordinates all ETL steps:
    1. Fetch items (labs/tasks) from external API
    2. Load them into local database (avoiding duplicates)
    3. Determine the last sync timestamp from existing logs
    4. Fetch only new logs since that timestamp (incremental sync)
    5. Load new logs with proper relations to learners and items
    6. Return statistics about what was processed
    
    Args:
        session: Async SQLAlchemy session for database operations
        
    Returns:
        dict with counts: new_records, total_records, new_items
    """
    
   
    print("🔄 [ETL] Starting items sync...")
    
    
    items = await fetch_items()
    print(f"📥 [ETL] Fetched {len(items)} items from AutoChecker API")
    
   
    new_items = await load_items(items, session)
    print(f"💾 [ETL] Created {new_items} new items in database")
    
    
    stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(stmt)
    last_sync = result.scalar_one()  
    
    
    since = last_sync if last_sync else None
    print(f"🕐 [ETL] Fetching logs since: {since or 'beginning of time'}")
    
    
    logs = await fetch_logs(since)
    print(f"📥 [ETL] Fetched {len(logs)} logs from AutoChecker API")
    
    
    new_logs = await load_logs(logs, items, session)
    print(f"💾 [ETL] Created {new_logs} new interaction logs")
    
    
    stmt = select(func.count()).select_from(InteractionLog)
    result = await session.execute(stmt)
    total_logs = result.scalar_one()
    
   =
    return {
        "new_records": new_logs,      
        "total_records": total_logs,  
        "new_items": new_items        
    }

