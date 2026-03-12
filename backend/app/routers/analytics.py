"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from sqlalchemy import and_, case, distinct

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns 4 buckets: 0-25, 26-50, 51-75, 76-100 with counts.
    """
    # Step 1: Find the lab item by matching title
    # Convert "lab-04" to title pattern like "%Lab 04%"
    lab_number = lab.replace("lab-", "")
    lab_title_pattern = f"%Lab {lab_number}%"
    
    lab_statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(lab_title_pattern)
    )
    lab_result = await session.execute(lab_statement)
    lab_item = lab_result.scalar_one_or_none()
    
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Step 2: Find all tasks belonging to this lab
    tasks_statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    tasks_result = await session.execute(tasks_statement)
    task_ids = [row[0] for row in tasks_result]
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Step 3: Query interactions and group into buckets
    bucket_0_25 = case(
        (InteractionLog.score <= 25, 1),
        else_=0
    )
    bucket_26_50 = case(
        (and_(InteractionLog.score > 25, InteractionLog.score <= 50), 1),
        else_=0
    )
    bucket_51_75 = case(
        (and_(InteractionLog.score > 50, InteractionLog.score <= 75), 1),
        else_=0
    )
    bucket_76_100 = case(
        (and_(InteractionLog.score > 75, InteractionLog.score <= 100), 1),
        else_=0
    )
    
    statement = select(
        func.sum(bucket_0_25).label("bucket_0_25"),
        func.sum(bucket_26_50).label("bucket_26_50"),
        func.sum(bucket_51_75).label("bucket_51_75"),
        func.sum(bucket_76_100).label("bucket_76_100")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    )
    
    result = await session.execute(statement)
    row = result.one()
    
    return [
        {"bucket": "0-25", "count": row.bucket_0_25 or 0},
        {"bucket": "26-50", "count": row.bucket_26_50 or 0},
        {"bucket": "51-75", "count": row.bucket_51_75 or 0},
        {"bucket": "76-100", "count": row.bucket_76_100 or 0}
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.
    
    Returns average score and attempt count for each task.
    """
    # Step 1: Find the lab item
    lab_number = lab.replace("lab-", "")
    lab_title_pattern = f"%Lab {lab_number}%"
    
    lab_statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(lab_title_pattern)
    )
    lab_result = await session.execute(lab_statement)
    lab_item = lab_result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    # Step 2: Find tasks and their stats
    statement = select(
        ItemRecord.title.label("task_title"),
        func.avg(InteractionLog.score).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(
        InteractionLog, InteractionLog.item_id == ItemRecord.id
    ).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task",
        InteractionLog.score.isnot(None)
    ).group_by(
        ItemRecord.id, ItemRecord.title
    ).order_by(
        ItemRecord.title
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    return [
        {
            "task": row.task_title,
            "avg_score": round(row.avg_score, 1) if row.avg_score else 0,
            "attempts": row.attempts
        }
        for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.
    
    Returns daily submission counts.
    """
    # Step 1: Find the lab item
    lab_number = lab.replace("lab-", "")
    lab_title_pattern = f"%Lab {lab_number}%"
    
    lab_statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(lab_title_pattern)
    )
    lab_result = await session.execute(lab_statement)
    lab_item = lab_result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    # Step 2: Find all task IDs for this lab
    tasks_statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    tasks_result = await session.execute(tasks_statement)
    task_ids = [row[0] for row in tasks_result]
    
    if not task_ids:
        return []
    
    # Step 3: Group by date
    statement = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at)
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    return [
        {
            "date": str(row.date),
            "submissions": row.submissions
        }
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.
    
    Returns average score and student count for each group.
    """
    # Step 1: Find the lab item
    lab_number = lab.replace("lab-", "")
    lab_title_pattern = f"%Lab {lab_number}%"
    
    lab_statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(lab_title_pattern)
    )
    lab_result = await session.execute(lab_statement)
    lab_item = lab_result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    # Step 2: Find all task IDs for this lab
    tasks_statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    tasks_result = await session.execute(tasks_statement)
    task_ids = [row[0] for row in tasks_result]
    
    if not task_ids:
        return []
    
    # Step 3: Join with learners and group by student_group
    statement = select(
        Learner.student_group.label("group"),
        func.avg(InteractionLog.score).label("avg_score"),
        func.count(distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None),
        Learner.student_group != ""  # Skip empty groups
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1) if row.avg_score else 0,
            "students": row.students
        }
        for row in rows
    ]
