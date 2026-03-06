"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from sqlalchemy import case
from fastapi import APIRouter, Depends, Query
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_fragment(lab: str) -> str:
    parts = lab.split("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return f"Lab {parts[1]}"
    return lab.replace("-", " ").title()


async def _find_lab_item(lab: str, session: AsyncSession) -> ItemRecord | None:
    fragment = _lab_title_fragment(lab)
    stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(fragment),
    )
    return (await session.exec(stmt)).first()


async def _task_ids_for_lab(lab_id: int, session: AsyncSession) -> list[int]:
    stmt = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_id,
    )
    return [item_id for item_id in (await session.exec(stmt)).all() if item_id is not None]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    default = [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]

    lab_item = await _find_lab_item(lab, session)
    if lab_item is None or lab_item.id is None:
        return default

    task_ids = await _task_ids_for_lab(lab_item.id, session)
    if not task_ids:
        return default

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(bucket_expr, func.count(InteractionLog.id).label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_expr)
    )
    rows = (await session.exec(stmt)).all()
    counts = {bucket: int(count) for bucket, count in rows}

    return [
        {"bucket": "0-25", "count": counts.get("0-25", 0)},
        {"bucket": "26-50", "count": counts.get("26-50", 0)},
        {"bucket": "51-75", "count": counts.get("51-75", 0)},
        {"bucket": "76-100", "count": counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    lab_item = await _find_lab_item(lab, session)
    if lab_item is None or lab_item.id is None:
        return []

    avg_score = func.round(func.avg(InteractionLog.score), 1).label("avg_score")
    attempts = func.count(InteractionLog.id).label("attempts")

    stmt = (
        select(ItemRecord.title, avg_score, attempts)
        .select_from(ItemRecord)
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id, isouter=True)
        .where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title.asc())
    )
    rows = (await session.exec(stmt)).all()

    return [
        {
            "task": title,
            "avg_score": float(score if score is not None else 0.0),
            "attempts": int(attempt_count),
        }
        for title, score, attempt_count in rows
    ]

"""
Try to commit
"""
@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    lab_item = await _find_lab_item(lab, session)
    if lab_item is None or lab_item.id is None:
        return []

    task_ids = await _task_ids_for_lab(lab_item.id, session)
    if not task_ids:
        return []

    day = func.date(InteractionLog.created_at).label("day")
    submissions = func.count(InteractionLog.id).label("submissions")
    stmt = (
        select(day, submissions)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(day)
        .order_by(day.asc())
    )
    rows = (await session.exec(stmt)).all()

    return [{"date": str(date_value), "submissions": int(total)} for date_value, total in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    lab_item = await _find_lab_item(lab, session)
    if lab_item is None or lab_item.id is None:
        return []

    task_ids = await _task_ids_for_lab(lab_item.id, session)
    if not task_ids:
        return []

    avg_score = func.round(func.avg(InteractionLog.score), 1).label("avg_score")
    students = func.count(func.distinct(Learner.id)).label("students")
    stmt = (
        select(Learner.student_group, avg_score, students)
        .select_from(InteractionLog)
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group.asc())
    )
    rows = (await session.exec(stmt)).all()

    return [
        {
            "group": group_name,
            "avg_score": float(score if score is not None else 0.0),
            "students": int(student_count),
        }
        for group_name, score, student_count in rows
    ]
