"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from sqlalchemy import func, case

from app.database import get_session
from app.models import Item, Interaction, Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""

    lab_title = lab.replace("lab-", "Lab ")

    lab_item = (await session.exec(
        select(Item).where(Item.title.contains(lab_title))
    )).first()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    task_ids = (await session.exec(
        select(Item.id).where(Item.parent_id == lab_item.id)
    )).all()

    stmt = select(
        case(
            (Interaction.score <= 25, "0-25"),
            (Interaction.score <= 50, "26-50"),
            (Interaction.score <= 75, "51-75"),
            else_="76-100",
        ).label("bucket"),
        func.count().label("count"),
    ).where(
        Interaction.item_id.in_(task_ids),
        Interaction.score != None
    ).group_by("bucket")

    rows = (await session.exec(stmt)).all()

    buckets = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0,
    }

    for bucket, count in rows:
        buckets[bucket] = count

    return [{"bucket": k, "count": v} for k, v in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""

    lab_title = lab.replace("lab-", "Lab ")

    lab_item = (await session.exec(
        select(Item).where(Item.title.contains(lab_title))
    )).first()

    if not lab_item:
        return []

    task_ids = (await session.exec(
        select(Item.id).where(Item.parent_id == lab_item.id)
    )).all()

    stmt = select(
        Item.title,
        func.round(func.avg(Interaction.score), 1).label("avg_score"),
        func.count().label("attempts"),
    ).join(
        Interaction, Interaction.item_id == Item.id
    ).where(
        Item.id.in_(task_ids)
    ).group_by(
        Item.title
    ).order_by(
        Item.title
    )

    rows = (await session.exec(stmt)).all()

    return [
        {
            "task": title,
            "avg_score": avg_score,
            "attempts": attempts,
        }
        for title, avg_score, attempts in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""

    lab_title = lab.replace("lab-", "Lab ")

    lab_item = (await session.exec(
        select(Item).where(Item.title.contains(lab_title))
    )).first()

    if not lab_item:
        return []

    task_ids = (await session.exec(
        select(Item.id).where(Item.parent_id == lab_item.id)
    )).all()

    stmt = select(
        func.date(Interaction.created_at).label("date"),
        func.count().label("submissions"),
    ).where(
        Interaction.item_id.in_(task_ids)
    ).group_by(
        "date"
    ).order_by(
        "date"
    )

    rows = (await session.exec(stmt)).all()

    return [
        {
            "date": str(date),
            "submissions": submissions,
        }
        for date, submissions in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""

    lab_title = lab.replace("lab-", "Lab ")

    lab_item = (await session.exec(
        select(Item).where(Item.title.contains(lab_title))
    )).first()

    if not lab_item:
        return []

    task_ids = (await session.exec(
        select(Item.id).where(Item.parent_id == lab_item.id)
    )).all()

    stmt = select(
        Learner.student_group,
        func.round(func.avg(Interaction.score), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students"),
    ).join(
        Interaction, Interaction.learner_id == Learner.id
    ).where(
        Interaction.item_id.in_(task_ids)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )

    rows = (await session.exec(stmt)).all()

    return [
        {
            "group": group,
            "avg_score": avg_score,
            "students": students,
        }
        for group, avg_score, students in rows
    ]