from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from sqlalchemy import func, case

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


def lab_to_title(lab: str) -> str:
    """Convert lab id like 'lab-04' to title fragment 'Lab 04'."""
    num = lab.split("-")[1]
    return f"Lab {num}"


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_to_title(lab)

    lab_query = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = lab_query.first()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    tasks_query = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = tasks_query.all()

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    query = await session.exec(
        select(bucket_case, func.count())
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(bucket_case)
    )

    results = dict(query.all())

    buckets = ["0-25", "26-50", "51-75", "76-100"]

    return [{"bucket": b, "count": results.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_to_title(lab)

    lab_query = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = lab_query.first()

    if not lab_item:
        return []

    query = await session.exec(
        select(
            ItemRecord.title,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(InteractionLog.id),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    return [
        {"task": title, "avg_score": avg_score, "attempts": attempts}
        for title, avg_score, attempts in query.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_to_title(lab)

    lab_query = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = lab_query.first()

    if not lab_item:
        return []

    tasks_query = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = tasks_query.all()

    query = await session.exec(
        select(
            func.date(InteractionLog.created_at),
            func.count(InteractionLog.id),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    return [
        {"date": str(date), "submissions": submissions}
        for date, submissions in query.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_to_title(lab)

    lab_query = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = lab_query.first()

    if not lab_item:
        return []

    tasks_query = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = tasks_query.all()

    query = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(func.distinct(Learner.id)),
        )
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    return [
        {"group": group, "avg_score": avg_score, "students": students}
        for group, avg_score, students in query.all()
    ]