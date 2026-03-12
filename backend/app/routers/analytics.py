"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, distinct
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


async def _get_lab_and_task_ids(session: AsyncSession, lab: str):
    """Find lab item and return its id and child task ids.
    
    The lab parameter is like 'lab-04', but the title is like 'Lab 04 — Testing'.
    We match by converting lab-04 to 'Lab 04' and checking if title contains it.
    """
    # Convert 'lab-04' to 'Lab 04' for matching
    lab_title_part = lab.replace('-', ' ').title().split(' —')[0]
    
    # Find the lab item
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%{lab_title_part}%")
    )
    lab_result = (await session.exec(lab_stmt)).first()
    
    if not lab_result:
        return None, []
    
    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_result.id
    )
    tasks = (await session.exec(tasks_stmt)).all()
    task_ids = [t.id for t in tasks]
    
    return lab_result.id, task_ids


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    - Find the lab item by matching title (e.g., "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    # Define buckets using CASE WHEN
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25"  # Default, shouldn't happen
    ).label("bucket")
    
    # Query with grouping by bucket
    stmt = select(
        bucket_expr,
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by("bucket")
    
    results = (await session.exec(stmt)).all()
    
    # Build result dict from query
    bucket_counts = {row.bucket: row.count for row in results}
    
    # Always return all four buckets
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.
    
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    # Get tasks with their stats
    stmt = select(
        ItemRecord.title.label("task"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(
        InteractionLog, InteractionLog.item_id == ItemRecord.id
    ).where(
        ItemRecord.id.in_(task_ids)
    ).group_by(
        ItemRecord.id, ItemRecord.title
    ).order_by(
        ItemRecord.title
    )
    
    results = (await session.exec(stmt)).all()
    
    return [
        {"task": row.task, "avg_score": float(row.avg_score) if row.avg_score else 0.0, "attempts": row.attempts}
        for row in results
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.
    
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    # Group by date - use func.date for SQLite compatibility
    stmt = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at)
    )
    
    results = (await session.exec(stmt)).all()
    
    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in results
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.
    
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    stmt = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    results = (await session.exec(stmt)).all()
    
    return [
        {"group": row.group, "avg_score": float(row.avg_score) if row.avg_score else 0.0, "students": row.students}
        for row in results
    ]
