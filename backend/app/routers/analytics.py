"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    # Transform lab-04 → Lab 04
    lab_title_pattern = f"%{lab.replace('lab-', 'Lab ').capitalize()}%"
    
    # Find the lab item
    lab_stmt = select(ItemRecord.id).where(ItemRecord.title.ilike(lab_title_pattern))
    lab_result = await session.exec(lab_stmt)
    lab_id_row = lab_result.first()
    lab_id = lab_id_row[0] if lab_id_row else None
    
    if lab_id is None:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [row[0] for row in task_ids_result.all()]
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Query interactions for these items that have a score
    # Group scores into buckets using CASE WHEN
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25"  # fallback
    ).label("bucket")

    scores_stmt = (
        select(bucket_expr, func.count(InteractionLog.id).label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None)
        )
        .group_by(bucket_expr)
    )

    scores_result = await session.exec(scores_stmt)
    bucket_counts = {row[0]: row[1] for row in scores_result.all()}

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
    # Transform lab-04 → Lab 04
    lab_title_pattern = f"%{lab.replace('lab-', 'Lab ').capitalize()}%"
    
    # Find the lab item
    lab_stmt = select(ItemRecord.id).where(ItemRecord.title.ilike(lab_title_pattern))
    lab_result = await session.exec(lab_stmt)
    lab_id_row = lab_result.first()
    lab_id = lab_id_row[0] if lab_id_row else None
    
    if lab_id is None:
        return []
    
    # Find all task items that belong to this lab
    tasks_stmt = (
        select(ItemRecord.id, ItemRecord.title)
        .where(ItemRecord.parent_id == lab_id)
        .order_by(ItemRecord.title)
    )
    tasks_result = await session.exec(tasks_stmt)
    tasks = tasks_result.all()

    if not tasks:
        return []

    result = []
    for task_row in tasks:
        task_id = task_row[0]
        task_title = task_row[1]
        # Compute avg_score and attempts for this task
        stats_stmt = (
            select(
                func.avg(InteractionLog.score).label("avg_score"),
                func.count(InteractionLog.id).label("attempts")
            )
            .where(InteractionLog.item_id == task_id)
        )
        stats_result = await session.exec(stats_stmt)
        stats = stats_result.first()

        avg_score = round(float(stats[0]), 1) if stats[0] is not None else 0.0
        attempts = stats[1] or 0
        
        result.append({
            "task": task_title,
            "avg_score": avg_score,
            "attempts": attempts,
        })
    
    return result


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
    # Transform lab-04 → Lab 04
    lab_title_pattern = f"%{lab.replace('lab-', 'Lab ').capitalize()}%"

    # Find the lab item
    lab_stmt = select(ItemRecord.id).where(ItemRecord.title.ilike(lab_title_pattern))
    lab_result = await session.exec(lab_stmt)
    lab_id_row = lab_result.first()
    lab_id = lab_id_row[0] if lab_id_row else None

    if lab_id is None:
        return []

    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [row[0] for row in task_ids_result.all()]

    if not task_ids:
        return []

    # Group interactions by date
    timeline_stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    timeline_result = await session.exec(timeline_stmt)

    return [
        {"date": str(row[0]), "submissions": row[1]}
        for row in timeline_result.all()
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
    # Transform lab-04 → Lab 04
    lab_title_pattern = f"%{lab.replace('lab-', 'Lab ').capitalize()}%"

    # Find the lab item
    lab_stmt = select(ItemRecord.id).where(ItemRecord.title.ilike(lab_title_pattern))
    lab_result = await session.exec(lab_stmt)
    lab_id_row = lab_result.first()
    lab_id = lab_id_row[0] if lab_id_row else None

    if lab_id is None:
        return []

    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [row[0] for row in task_ids_result.all()]

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    groups_stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    groups_result = await session.exec(groups_stmt)

    return [
        {
            "group": row[0],
            "avg_score": round(float(row[1]), 1) if row[1] is not None else 0.0,
            "students": row[2],
        }
        for row in groups_result.all()
    ]
