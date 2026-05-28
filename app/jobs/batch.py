import asyncio
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from app.models import Job

logger = structlog.get_logger()

async def process_batch_job(job_id: str, payload: dict, db: AsyncSession) -> dict:
    items = payload.get("items", ["item1", "item2", "item3", "item4", "item5"])
    total_items = len(items)
    processed_items = []
    
    log = logger.bind(job_id=str(job_id), total_items=total_items)
    log.info("Starting batch job processing loop")
    
    for index, item in enumerate(items):
        await asyncio.sleep(1)
        processed_items.append({"item": item, "status": "success"})
        
        current_progress = int(((index + 1) / total_items) * 100)
        
        await db.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(progress=current_progress)
        )
        await db.commit()
        
        log.info("Batch job progress updated", progress=f"{current_progress}%")
        
    return {
        "status": "batch_completed",
        "total_processed": total_items,
        "details": processed_items
    }
