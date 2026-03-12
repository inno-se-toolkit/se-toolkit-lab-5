from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy import text, select
from datetime import date

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab.replace("-", " ").title()
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )
    tasks = result.scalars().all()
    
    if not tasks:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    task_ids = [str(task.id) for task in tasks]
    task_ids_str = ",".join(task_ids)
    
    query = f"""
    SELECT 
        COUNT(CASE WHEN score >= 0 AND score <= 25 THEN 1 END) as bucket_0_25,
        COUNT(CASE WHEN score >= 26 AND score <= 50 THEN 1 END) as bucket_26_50,
        COUNT(CASE WHEN score >= 51 AND score <= 75 THEN 1 END) as bucket_51_75,
        COUNT(CASE WHEN score >= 76 AND score <= 100 THEN 1 END) as bucket_76_100
    FROM interacts 
    WHERE item_id IN ({task_ids_str}) AND score IS NOT NULL
    """
    
    result = await session.execute(text(query))
    row = result.first()
    
    return [
        {"bucket": "0-25", "count": row[0] if row[0] else 0},
        {"bucket": "26-50", "count": row[1] if row[1] else 0},
        {"bucket": "51-75", "count": row[2] if row[2] else 0},
        {"bucket": "76-100", "count": row[3] if row[3] else 0},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab.replace("-", " ").title()
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )
    tasks = result.scalars().all()
    
    if not tasks:
        return []
    
    result_list = []
    
    for task in tasks:
        result = await session.execute(
            select(InteractionLog).where(InteractionLog.item_id == task.id)
        )
        interactions = result.scalars().all()
        
        if interactions:
            scores = [i.score for i in interactions if i.score is not None]
            avg_score = round(sum(scores) / len(scores), 1) if scores else 0
            attempts = len(interactions)
        else:
            avg_score = 0
            attempts = 0
        
        result_list.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts
        })
    
    result_list.sort(key=lambda x: x["task"])
    
    return result_list


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab.replace("-", " ").title()
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )
    tasks = result.scalars().all()
    
    if not tasks:
        return []
    
    task_ids = [str(task.id) for task in tasks]
    task_ids_str = ",".join(task_ids)
    
    query = f"""
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as submissions
    FROM interacts 
    WHERE item_id IN ({task_ids_str})
    GROUP BY DATE(created_at)
    ORDER BY date ASC
    """
    
    result = await session.execute(text(query))
    rows = result.all()
    
    return [
        {"date": str(row[0]), "submissions": row[1]}
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab.replace("-", " ").title()
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    result = await session.execute(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )
    tasks = result.scalars().all()
    
    if not tasks:
        return []
    
    task_ids = [str(task.id) for task in tasks]
    task_ids_str = ",".join(task_ids)
    
    query = f"""
    SELECT 
        l.student_group as group_name,
        COALESCE(ROUND(AVG(i.score), 1), 0) as avg_score,
        COUNT(DISTINCT l.id) as students
    FROM learner l
    LEFT JOIN interacts i ON l.id = i.learner_id AND i.item_id IN ({task_ids_str}) AND i.score IS NOT NULL
    GROUP BY l.student_group
    HAVING l.student_group != ''
    ORDER BY group_name ASC
    """
    
    result = await session.execute(text(query))
    rows = result.all()
    
    return [
        {"group": row[0], "avg_score": float(row[1]), "students": row[2]}
        for row in rows
    ]