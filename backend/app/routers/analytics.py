"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession as SQLModelAsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


def _get_lab_title(lab: str) -> str:
    """Transform lab-04 → Lab 04."""
    parts = lab.split('-')
    if len(parts) == 2 and parts[0] == 'lab':
        return f"Lab {parts[1]}"
    return lab.replace("-", " ").title()


async def _get_lab_item(session: AsyncSession, lab_title: str) -> ItemRecord | None:
    """Find the lab item by title."""
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title == lab_title
    )
    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _get_task_ids(session: AsyncSession, lab_id: int) -> list[int]:
    """Get all task IDs that belong to a lab."""
    statement = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_id,
        ItemRecord.type == "task"
    )
    result = await session.execute(statement)
    return result.scalars().all()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: SQLModelAsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    # Always return all four buckets
    buckets = [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]
    
    # Find the lab
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return buckets
    
    # Find all tasks for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return buckets
    
    # Create bucket case expression
    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
    ).label("bucket")
    
    # Query to count interactions per bucket
    statement = select(
        bucket_case,
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by("bucket")
    
    result = await session.execute(statement)
    
    # Update bucket counts
    for row in result:
        for bucket in buckets:
            if bucket["bucket"] == row.bucket:
                bucket["count"] = row.count
                break
    
    return buckets


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: SQLModelAsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    # Find the lab
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []
    
    # Find all tasks for this lab, ordered by title
    statement = select(ItemRecord).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    ).order_by(ItemRecord.title)
    
    result = await session.execute(statement)
    tasks = result.scalars().all()
    
    # For each task, calculate average score and attempt count
    task_stats = []
    for task in tasks:
        stats_stmt = select(
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        
        stats_result = await session.execute(stats_stmt)
        row = stats_result.first()
        
        if row:
            avg_score = row.avg_score if row.avg_score is not None else 0.0
            # Round to 1 decimal place
            avg_score = round(avg_score, 1)
            
            task_stats.append({
                "task": task.title,
                "avg_score": float(avg_score),
                "attempts": row.attempts or 0,
            })
    
    return task_stats


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: SQLModelAsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    # Find the lab
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []
    
    # Find all tasks for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []
    
    # Group by date and count submissions
    # Use date() function to extract date from datetime
    date_func = func.date(InteractionLog.created_at).label("date")
    
    statement = select(
        date_func,
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        date_func
    ).order_by(
        date_func
    )
    
    result = await session.execute(statement)
    
    # Format results
    timeline = []
    for row in result:
        timeline.append({
            "date": str(row.date),
            "submissions": row.submissions
        })
    
    return timeline


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: SQLModelAsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    # Find the lab
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []
    
    # Find all tasks for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []
    
    # Join with learners and calculate stats per group
    statement = select(
        Learner.student_group.label("group"),
        func.avg(InteractionLog.score).label("avg_score"),
        func.count(func.distinct(InteractionLog.learner_id)).label("students")
    ).join(
        Learner, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.execute(statement)
    
    # Format results
    groups = []
    for row in result:
        avg_score = row.avg_score if row.avg_score is not None else 0.0
        # Round to 1 decimal place
        avg_score = round(avg_score, 1)
        
        groups.append({
            "group": row.group,
            "avg_score": float(avg_score),
            "students": row.students,
        })
    
    return groups