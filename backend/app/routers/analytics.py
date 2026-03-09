"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_fragment(lab: str) -> str:
    if lab.startswith("lab-"):
        return f"Lab {lab[4:]}"
    return lab.replace("-", " ").title()


async def _get_lab_record(lab: str, session: AsyncSession) -> ItemRecord | None:
    fragment = _lab_title_fragment(lab)
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(fragment),
        )
    )
    return result.first()


async def _get_task_ids(lab: str, session: AsyncSession) -> list[int]:
    lab_record = await _get_lab_record(lab, session)
    if lab_record is None:
        return []

    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_record.id,
        )
    )
    return list(result.all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    task_ids = await _get_task_ids(lab, session)

    buckets = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0,
    }

    if not task_ids:
        return [{"bucket": bucket, "count": count} for bucket, count in buckets.items()]

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    result = await session.exec(
        select(bucket_expr, func.count(InteractionLog.id))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_expr)
    )

    for bucket, count in result.all():
        buckets[bucket] = count

    return [{"bucket": bucket, "count": count} for bucket, count in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_record = await _get_lab_record(lab, session)
    if lab_record is None:
        return []

    result = await session.exec(
        select(
            ItemRecord.title,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .select_from(ItemRecord)
        .outerjoin(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_record.id,
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    return [
        {
            "task": task,
            "avg_score": float(avg_score) if avg_score is not None else None,
            "attempts": attempts,
        }
        for task, avg_score, attempts in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    task_ids = await _get_task_ids(lab, session)
    if not task_ids:
        return []

    day_expr = func.date(InteractionLog.created_at).label("date")

    result = await session.exec(
        select(
            day_expr,
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(day_expr)
        .order_by(day_expr)
    )

    return [
        {"date": str(date_value), "submissions": submissions}
        for date_value, submissions in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    task_ids = await _get_task_ids(lab, session)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .select_from(InteractionLog)
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    return [
        {
            "group": group,
            "avg_score": float(avg_score) if avg_score is not None else None,
            "students": students,
        }
        for group, avg_score, students in result.all()
    ]
