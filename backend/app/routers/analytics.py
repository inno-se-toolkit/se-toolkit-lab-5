"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import Session, select
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
    # Transform lab-04 → Lab 04 for title matching
    lab_title_pattern = f"Lab {lab.replace('lab-', '')}%"

    # Find the lab item
    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern)
        )
    )
    lab_record = lab_item.first()

    if not lab_record:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Find all task items belonging to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_record.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions with scores for these tasks
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25",  # Default for NULL or out-of-range
    ).label("bucket")

    query = (
        select(bucket_expr, func.count(InteractionLog.id).label("count"))
        .where(
            InteractionLog.item_id.in_(task_id_list),
            InteractionLog.score.isnot(None)
        )
        .group_by(bucket_expr)
    )

    result = await session.exec(query)
    bucket_counts = {row.bucket: row.count for row in result.all()}

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

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title_pattern = f"Lab {lab.replace('lab-', '')}%"

    # Find the lab item
    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern)
        )
    )
    lab_record = lab_item.first()

    if not lab_record:
        return []

    # Find all task items belonging to this lab
    tasks = await session.exec(
        select(ItemRecord)
        .where(ItemRecord.parent_id == lab_record.id)
        .order_by(ItemRecord.title)
    )

    result = []
    for task in tasks.all():
        # Query interactions for this task
        interactions = await session.exec(
            select(InteractionLog).where(InteractionLog.item_id == task.id)
        )
        interaction_list = list(interactions.all())

        attempts = len(interaction_list)
        if attempts > 0:
            avg_score = round(
                sum(i.score for i in interaction_list if i.score is not None) / attempts,
                1
            )
        else:
            avg_score = 0.0

        result.append({
            "task": task.title,
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

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title_pattern = f"Lab {lab.replace('lab-', '')}%"

    # Find the lab item
    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern)
        )
    )
    lab_record = lab_item.first()

    if not lab_record:
        return []

    # Find all task items belonging to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_record.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return []

    # Group interactions by date
    date_expr = func.date(InteractionLog.created_at).label("date")
    query = (
        select(date_expr, func.count(InteractionLog.id).label("submissions"))
        .where(InteractionLog.item_id.in_(task_id_list))
        .group_by(date_expr)
        .order_by(date_expr)
    )

    result = await session.exec(query)

    return [
        {"date": row.date, "submissions": row.submissions}
        for row in result.all()
    ]


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
    # Transform lab-04 → Lab 04 for title matching
    lab_title_pattern = f"Lab {lab.replace('lab-', '')}%"

    # Find the lab item
    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern)
        )
    )
    lab_record = lab_item.first()

    if not lab_record:
        return []

    # Find all task items belonging to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_record.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return []

    # Join interactions with learners and group by student_group
    query = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_id_list))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(query)

    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1) if row.avg_score else 0.0,
            "students": row.students,
        }
        for row in result.all()
    ]
