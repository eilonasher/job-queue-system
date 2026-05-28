import time
import uuid
from fastapi import FastAPI, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
import structlog

from app.database import get_db, engine, Base
from app.models import Job
from app.schemas import JobCreate, JobResponse, JobStatus
from app.database import get_db, engine, Base, get_redis
from app.queue import JobQueue 
from app.queue import QUEUE_PENDING, QUEUE_SCHEDULED

# Observability: use structLog to print logs that can be easily moitored. 
logger = structlog.get_logger()

# initialize fast api
app = FastAPI(
    title="Production-Ready Distributed Job Queue API",
    version="1.0.0",
    docs_url="/docs"
)

# create tables if they are not exist
@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialized successfully.")


# 1. Health Check Endpoint 
@app.get("/api/v1/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {
        "status": "healthy",
        "queue_stats": {
            "pending_count": 0,
            "processing_count": 0
        }
    }


# 2. Submit Job Endpoint 
@app.post(
    "/api/v1/jobs", 
    response_model=JobResponse, 
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new background job"
)
async def submit_job(
    job_data: JobCreate,
    x_idempotency_key: str = Header(..., alias="X-Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
    redis_cli = Depends(get_redis) 
):
    log = logger.bind(idempotency_key=x_idempotency_key, job_type=job_data.job_type)
    log.info("Received job submission request")

    initial_status = JobStatus.SCHEDULED.value if job_data.scheduled_at else JobStatus.PENDING.value

    new_job = Job(
        job_type=job_data.job_type,
        payload=job_data.payload,
        priority=job_data.priority,
        idempotency_key=x_idempotency_key,
        status=initial_status,
        scheduled_at=job_data.scheduled_at
    )

    try:
        db.add(new_job)
        await db.flush() 
        
        queue = JobQueue(redis_cli)
        scheduled_timestamp = job_data.scheduled_at.timestamp() if job_data.scheduled_at else None
        
        await queue.enqueue_job(
            job_id=str(new_job.id), 
            priority=new_job.priority, 
            scheduled_at_timestamp=scheduled_timestamp
        )
        
        log.info("Job successfully created and enqueued", job_id=str(new_job.id))
        return new_job

    except IntegrityError:
        await db.rollback()
        log.warning("Idempotency key collision detected. Fetching existing job.")

        query = select(Job).where(Job.idempotency_key == x_idempotency_key)
        result = await db.execute(query)
        existing_job = result.scalars().first()

        if not existing_job:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database integrity error"
            )

        return existing_job

# 3. Get Job Status Endpoint
@app.get(
    "/api/v1/jobs/{job_id}", 
    response_model=JobResponse, 
    status_code=status.HTTP_200_OK,
    summary="Get job status and results"
)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    logger.info("Fetching job status", job_id=str(job_id))
    
    query = select(Job).where(Job.id == job_id)
    result = await db.execute(query)
    job = result.scalars().first()
    
    if not job:
        logger.warning("Job not found", job_id=str(job_id))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Job not found"
        )
        
    return job


# 4. Cancel Job Endpoint
@app.post(
    "/api/v1/jobs/{job_id}/cancel", 
    status_code=status.HTTP_200_OK,
    summary="Cancel a pending or scheduled job"
)
async def cancel_job(
    job_id: uuid.UUID, 
    db: AsyncSession = Depends(get_db),
    redis_cli = Depends(get_redis)
):
    log = logger.bind(job_id=str(job_id))
    log.info("Received request to cancel job")
    
    query = select(Job).where(Job.id == job_id)
    result = await db.execute(query)
    job = result.scalars().first()
    
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        
    #if the job is running it CAN NOT be canceled    
    if job.status not in [JobStatus.PENDING.value, JobStatus.SCHEDULED.value]:
        log.warning("Cannot cancel job in its current status", current_status=job.status)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Cannot cancel a job that is already {job.status}"
        )
        
    job.status = JobStatus.CANCELLED.value
    await db.flush()
    
    async with redis_cli.pipeline() as pipe:
        pipe.zrem(QUEUE_PENDING, str(job_id))
        pipe.zrem(QUEUE_SCHEDULED, str(job_id))
        await pipe.execute()
        
    log.info("Job successfully cancelled and removed from Redis queues")
    return {"message": "Job successfully cancelled"}


# 5. Manual Retry Endpoint
@app.post(
    "/api/v1/jobs/{job_id}/retry", 
    response_model=JobResponse, 
    status_code=status.HTTP_200_OK,
    summary="Manually retry a failed or cancelled job"
)
async def retry_job(
    job_id: uuid.UUID, 
    db: AsyncSession = Depends(get_db),
    redis_cli = Depends(get_redis)
):
    log = logger.bind(job_id=str(job_id))
    log.info("Received request for manual retry")
    
    query = select(Job).where(Job.id == job_id)
    result = await db.execute(query)
    job = result.scalars().first()
    
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        
    if job.status not in [JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Cannot retry a job with status {job.status}"
        )
        
    job.status = JobStatus.PENDING.value
    job.current_attempts = 0
    job.error_info = None  
    job.progress = 0
    await db.flush()
    
    queue = JobQueue(redis_cli)
    await queue.enqueue_job(job_id=str(job.id), priority=job.priority)
    
    log.info("Job status reset to PENDING and re-enqueued successfully")
    return job
