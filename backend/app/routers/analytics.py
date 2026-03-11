"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner


router = APIRouter()

def lab_param_to_title_fragment(lab: str) -> str:
    """Transform 'lab-04' -> 'Lab 04' to match item titles."""
    prefix, num = lab.split("-")
    return f"{prefix.capitalize()} {num}"


async def get_lab_task_ids(session: AsyncSession, lab: str) -> list[int]:
    """Find lab item and return IDs of its child task items."""
    title_fragment = lab_param_to_title_fragment(lab)

    # Находим lab по title
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(title_fragment))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.scalars().first()
    if lab_item is None:
        return []

    # Находим дочерние задачи по parent_id
    tasks_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    task_ids = [row[0] for row in tasks_result.all()]
    return task_ids




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
    task_ids = await get_lab_task_ids(session, lab)
    buckets = ["0-25", "26-50", "51-75", "76-100"]

    if not task_ids:
        return [{"bucket": b, "count": 0} for b in buckets]

    score_col = InteractionLog.score

    bucket_case = case(
        (score_col <= 25, "0-25"),
        (score_col <= 50, "26-50"),
        (score_col <= 75, "51-75"),
        else_="76-100",
    )

    stmt = (
        select(bucket_case.label("bucket"), func.count().label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(score_col.is_not(None))
        .group_by("bucket")
    )

    result = await session.exec(stmt)
    rows = result.all()

    counts = {b: 0 for b in buckets}
    for bucket, count in rows:
        counts[bucket] = count

    return [{"bucket": b, "count": counts[b]} for b in buckets]


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
    task_ids = await get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.id.in_(task_ids))
        .where(InteractionLog.score.is_not(None))
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {
            "task": task,
            "avg_score": float(avg_score) if avg_score is not None else 0.0,
            "attempts": attempts,
        }
        for task, avg_score, attempts in rows
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
    task_ids = await get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    date_expr = func.date(InteractionLog.created_at)

    stmt = (
        select(
            date_expr.label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by("date")
        .order_by("date")
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {
            "date": date,
            "submissions": submissions,
        }
        for date, submissions in rows
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
    task_ids = await get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.is_not(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {
            "group": group,
            "avg_score": float(avg_score) if avg_score is not None else 0.0,
            "students": students,
        }
        for group, avg_score, students in rows
    ]
