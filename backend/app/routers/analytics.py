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
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


async def _get_lab_tasks(lab: str, session: AsyncSession) -> list[int]:
    """Get task IDs for a given lab short ID (e.g., 'lab-04')."""
    # Normalize lab param: "lab-04" → "Lab 04"
    lab_title_part = lab.replace("-", " ").title()

    # Find the lab item by matching title
    stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(f"%{lab_title_part}%")
    )
    result = await session.execute(stmt)
    lab_item = result.scalars().first()

    if not lab_item:
        return []

    # Get all task items belonging to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.execute(tasks_stmt)
    return [t.id for t in tasks_result.scalars().all()]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    task_ids = await _get_lab_tasks(lab, session)

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Build bucket aggregation using CASE WHEN
    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(bucket_case, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None)
        )
        .group_by(bucket_case)
    )

    results = await session.execute(stmt)
    bucket_counts = {row.bucket: row.count for row in results.all()}

    # Ensure all four buckets are present
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
    # Normalize lab param: "lab-04" → "Lab 04"
    lab_title_part = lab.replace("-", " ").title()

    # Find the lab item by matching title
    stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(f"%{lab_title_part}%")
    )
    result = await session.execute(stmt)
    lab_item = result.scalars().first()

    if not lab_item:
        return []

    # Find all task items belonging to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id).order_by(ItemRecord.title)
    tasks_result = await session.execute(tasks_stmt)

    result_list = []
    for task in tasks_result.scalars().all():
        # Get stats for this task
        stats_stmt = (
            select(
                func.avg(InteractionLog.score).label("avg_score"),
                func.count().label("attempts")
            )
            .where(
                InteractionLog.item_id == task.id,
                InteractionLog.score.isnot(None)
            )
        )
        stats_result = await session.execute(stats_stmt)
        stats = stats_result.first()

        if stats and stats.attempts > 0:
            result_list.append({
                "task": task.title,
                "avg_score": round(float(stats.avg_score), 1) if stats.avg_score else 0.0,
                "attempts": stats.attempts,
            })

    return result_list


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    task_ids = await _get_lab_tasks(lab, session)

    if not task_ids:
        return []

    # Group by date and count submissions
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        )
        .where(
            InteractionLog.item_id.in_(task_ids),
        )
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    results = await session.execute(stmt)

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in results.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    task_ids = await _get_lab_tasks(lab, session)

    if not task_ids:
        return []

    # Join interactions with learners and aggregate by group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students")
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None)
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    results = await session.execute(stmt)

    return [
        {
            "group": row.group,
            "avg_score": round(float(row.avg_score), 1) if row.avg_score else 0.0,
            "students": row.students,
        }
        for row in results.all()
    ]
