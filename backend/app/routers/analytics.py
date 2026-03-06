"""Router for analytics endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, distinct
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_filter(lab: str) -> str:
    parts = lab.split("-")
    return f"Lab {parts[1]}"


async def _get_task_ids(lab: str, session: AsyncSession) -> list[int]:
    lab_title = _lab_title_filter(lab)
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = result.first()
    if not lab_item:
        return []
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    tasks = result.all()
    return [t.id for t in tasks]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_task_ids(lab, session)
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )
    result = await session.exec(
        select(bucket_expr, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_expr)
    )
    rows = result.all()
    counts = {row[0]: row[1] for row in rows}
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": b, "count": counts.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_task_ids(lab, session)
    result = await session.exec(
        select(
            ItemRecord.title,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    rows = result.all()
    return [
        {"task": row[0], "avg_score": round(float(row[1]), 1), "attempts": row[2]}
        for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_task_ids(lab, session)
    date_expr = func.date(InteractionLog.created_at)
    result = await session.exec(
        select(date_expr.label("date"), func.count().label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )
    rows = result.all()
    return [{"date": str(row[0])[:10], "submissions": row[1]} for row in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_task_ids(lab, session)
    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    rows = result.all()
    return [
        {"group": row[0], "avg_score": round(float(row[1]), 1), "students": row[2]}
        for row in rows
    ]
