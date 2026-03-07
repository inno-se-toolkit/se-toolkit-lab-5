"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

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

    - Find the lab item by matching title (e.g., "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    # Convert lab-04 → "Lab 04" for title matching
    lab_title = f"Lab {lab.split('-')[1]}"

    # Find the lab item - use scalars() to get model instances
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title),
    )
    lab_result = await session.exec(lab_stmt)
    lab_items = lab_result.scalars().all()
    lab_item = lab_items[0] if lab_items else None

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = list(task_ids_result.scalars().all())

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions for these items with scores
    score_bucket = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25",
    ).label("bucket")

    stmt = (
        select(score_bucket, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None),
        )
        .group_by(score_bucket)
    )

    result = await session.exec(stmt)
    bucket_counts = {row.bucket: row.count for row in result}

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
    # Convert lab-04 → "Lab 04" for title matching
    lab_title = f"Lab {lab.split('-')[1]}"

    # Find the lab item - use scalars() to get model instances
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title),
    )
    lab_result = await session.exec(lab_stmt)
    lab_items = lab_result.scalars().all()
    lab_item = lab_items[0] if lab_items else None

    if not lab_item:
        return []

    # Find all task items that belong to this lab
    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count().label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task",
        )
        .group_by(ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.exec(stmt)

    return [
        {"task": row.task, "avg_score": float(row.avg_score), "attempts": row.attempts}
        for row in result
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
    # Convert lab-04 → "Lab 04" for title matching
    lab_title = f"Lab {lab.split('-')[1]}"

    # Find the lab item - use scalars() to get model instances
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title),
    )
    lab_result = await session.exec(lab_stmt)
    lab_items = lab_result.scalars().all()
    lab_item = lab_items[0] if lab_items else None

    if not lab_item:
        return []

    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = list(task_ids_result.scalars().all())

    if not task_ids:
        return []

    # Group interactions by date
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.exec(stmt)

    return [
        {"date": str(row.date), "submissions": row.submissions} for row in result
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
    # Convert lab-04 → "Lab 04" for title matching
    lab_title = f"Lab {lab.split('-')[1]}"

    # Find the lab item - use scalars() to get model instances
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title),
    )
    lab_result = await session.exec(lab_stmt)
    lab_items = lab_result.scalars().all()
    lab_item = lab_items[0] if lab_items else None

    if not lab_item:
        return []

    # Find all task items that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = list(task_ids_result.scalars().all())

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)

    return [
        {"group": row.group, "avg_score": float(row.avg_score), "students": row.students}
        for row in result
    ]
