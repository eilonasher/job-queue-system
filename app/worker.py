from datetime import datetime, timedelta
from sqlalchemy import delete
import asyncio
import time
from datetime import datetime, timedelta
import structlog
from sqlalchemy.future import select
from sqlalchemy import update

from app.database import AsyncSessionLocal, redis_client
from app.models import Job
from app.schemas import JobStatus, JobType

from app.jobs.email import process_email_job
from app.jobs.webhook import process_webhook_job
from app.jobs.report import process_report_job
from app.jobs.batch import process_batch_job
from app.queue import QUEUE_PENDING, JobQueue

logger = structlog.get_logger()

async def fetch_next_job_id() -> str | None:
    """
    get the next job to executed, use zrem to avoid race condition betweeen workers
    """
    results = await redis_client.zrevrange(QUEUE_PENDING, 0, 0)
    if not results:
        return None
        
    job_id = results[0]
    
    removed = await redis_client.zrem(QUEUE_PENDING, job_id)
    if removed > 0:
        return job_id
        
    return None

async def handle_job_failure(job: Job, error_msg: str, db: AsyncSessionLocal):
    """
    Failed or Backoff
    """
    job.current_attempts += 1
    job.error_info = error_msg
    queue = JobQueue(redis_client)
    
    if job.current_attempts < job.max_attempts:
        backoff_delay = (2 ** job.current_attempts) * 2
        run_at_timestamp = time.time() + backoff_delay
        scheduled_time = datetime.utcnow() + timedelta(seconds=backoff_delay)
        
        job.status = JobStatus.SCHEDULED.value
        job.scheduled_at = scheduled_time
        
        await queue.enqueue_job(str(job.id), job.priority, scheduled_at_timestamp=run_at_timestamp)
        logger.warning("Job failed, scheduled for retry (Backoff)", job_id=str(job.id), attempt=job.current_attempts, delay=backoff_delay)
    else:
        job.status = JobStatus.FAILED.value
        job.completed_at = datetime.utcnow()
        logger.error("Job failed permanently after max attempts", job_id=str(job.id))
        
    await db.commit()

async def process_job_lifecycle(job_id: str):
    """
    Manage job lifecycle
    """
    log = logger.bind(job_id=job_id)
    
    async with AsyncSessionLocal() as db:
        query = select(Job).where(Job.id == job_id)
        result = await db.execute(query)
        job = result.scalars().first()
        
        if not job or job.status == JobStatus.CANCELLED.value:
            log.info("Job was cancelled or deleted before execution. Skipping.")
            return

        job.status = JobStatus.PROCESSING.value
        job.started_at = datetime.utcnow()
        await db.commit()
        log.info("Job status updated to PROCESSING", job_type=job.job_type)
        
        try:
            if job.job_type == JobType.EMAIL.value:
                res_data = await process_email_job(job.payload)
            elif job.job_type == JobType.WEBHOOK.value:
                res_data = await process_webhook_job(job.payload)
            elif job.job_type == JobType.REPORT.value:
                res_data = await process_report_job(job.payload)
            elif job.job_type == JobType.BATCH.value:
                res_data = await process_batch_job(str(job.id), job.payload, db)
            else:
                raise ValueError(f"Unknown job type: {job.job_type}")
                
            job.status = JobStatus.COMPLETED.value
            job.result = res_data
            job.completed_at = datetime.utcnow()
            job.progress = 100
            await db.commit()
            log.info("Job completed successfully!")
            
        except Exception as e:
            await db.rollback()
            await handle_job_failure(job, str(e), db)

async def run_cleanup_retention():
    """
    delete finished jobs that executed over 24 hours ago
    """
    logger.info("Starting database retention cleanup task...")

    time_threshold = datetime.utcnow() - timedelta(hours=24)

    async with AsyncSessionLocal() as db:
        try:
            stmt = (
                delete(Job)
                .where(Job.created_at < time_threshold)
                .where(Job.status.in_([
                    JobStatus.COMPLETED.value, 
                    JobStatus.FAILED.value, 
                    JobStatus.CANCELLED.value
                ]))
            )
            result = await db.execute(stmt)
            await db.commit()

            logger.info(
                "Database retention cleanup completed successfully", 
                rows_deleted=result.rowcount
            )
        except Exception as e:
            await db.rollback()
            logger.error("Failed to run database cleanup task", error=str(e))

async def worker_loop():
    """
    main loop of the worker
    """
    logger.info("Worker process started and listening for jobs...")
    queue = JobQueue(redis_client)

    last_cleanup_time = 0
    CLEANUP_INTERVAL_SECONDS = 3600

    while True:
        try:
            current_time = time.time()

            # 1. for test its one hour and not 24 hours
            if current_time - last_cleanup_time > CLEANUP_INTERVAL_SECONDS:
                await run_cleanup_retention()
                last_cleanup_time = current_time

            # 2. Scheduled to pending
            await queue.move_scheduled_to_pending()

            # 3. get next job to execute
            job_id = await fetch_next_job_id()
            if job_id:
                await process_job_lifecycle(job_id)
            else:
                # avoid cpu over usage
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.critical("Critical error in worker loop", error=str(e))
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(worker_loop())
