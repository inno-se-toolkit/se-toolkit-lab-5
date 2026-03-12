"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


def _normalize_lab_id(lab: str) -> str:
    """Transform lab identifier to match title format.

    Examples:
    - "lab-04" → "Lab 04"
    - "lab-1" → "Lab 1"
    """
    parts = lab.split("-")
    if len(parts) == 2 and parts[0] == "lab":
        # Convert "lab-04" to "Lab 04"
        return f"Lab {parts[1].lstrip('0') or '0'}"
    return lab


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    # Normalize lab identifier to match title format
    normalized_lab = _normalize_lab_id(lab)

    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(normalized_lab)
    )
    result = await session.execute(lab_query)
    lab_item = result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all task items for this lab
    task_query = select(ItemRecord.id).where(
        ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(task_query)
    task_ids = [row[0] for row in result.all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions for these tasks with scores, grouped into buckets
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
    )

    scores_query = (
        select(
            bucket_expr.label("bucket"), func.count(InteractionLog.id).label("count")
        )
        .where(InteractionLog.item_id.in_(task_ids), InteractionLog.score.is_not(None))
        .group_by(bucket_expr)
    )

    result = await session.execute(scores_query)
    rows = result.all()

    # Build result with all buckets
    bucket_counts = {row[0]: row[1] for row in rows}
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
    """Per-task pass rates for a given lab."""
    # Normalize lab identifier to match title format
    normalized_lab = _normalize_lab_id(lab)

    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(normalized_lab)
    )
    result = await session.execute(lab_query)
    lab_item = result.scalar_one_or_none()

    if not lab_item:
        return []

    # Query pass rates for each task
    pass_rates_query = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, ItemRecord.id == InteractionLog.item_id)
        .where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
            InteractionLog.score.is_not(None),
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.execute(pass_rates_query)
    rows = result.all()

    return [
        {"task": row[0], "avg_score": float(row[1]), "attempts": row[2]} for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    # Normalize lab identifier to match title format
    normalized_lab = _normalize_lab_id(lab)

    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(normalized_lab)
    )
    result = await session.execute(lab_query)
    lab_item = result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all task items for this lab
    task_query = select(ItemRecord.id).where(
        ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(task_query)
    task_ids = [row[0] for row in result.all()]

    if not task_ids:
        return []

    # Query submissions per day
    timeline_query = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.execute(timeline_query)
    rows = result.all()

    return [{"date": str(row[0]), "submissions": row[1]} for row in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    # Normalize lab identifier to match title format
    normalized_lab = _normalize_lab_id(lab)

    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(normalized_lab)
    )
    result = await session.execute(lab_query)
    lab_item = result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all task items for this lab
    task_query = select(ItemRecord.id).where(
        ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(task_query)
    task_ids = [row[0] for row in result.all()]

    if not task_ids:
        return []

    # Query group statistics
    groups_query = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids), InteractionLog.score.is_not(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.execute(groups_query)
    rows = result.all()

    return [
        {"group": row[0], "avg_score": float(row[1]), "students": row[2]}
        for row in rows
    ]
