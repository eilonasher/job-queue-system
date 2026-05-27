import uuid
from fastapi import FastAPI, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
import structlog

from app.database import get_db, engine, Base
from app.models import Job
from app.schemas import JobCreate, JobResponse, JobStatus

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
    db: AsyncSession = Depends(get_db)
):
    log = logger.bind(idempotency_key=x_idempotency_key, job_type=job_data.job_type)
    log.info("Received job submission request")

    new_job = Job(
        job_type=job_data.job_type,
        payload=job_data.payload,
        priority=job_data.priority,
        idempotency_key=x_idempotency_key,
        status=JobStatus.PENDING.value,
        status=JobStatus.SCHEDULED.value if job_data.scheduled_at else JobStatus.PENDING.value,
        scheduled_at=job_data.scheduled_at
    )

    try:
        db.add(new_job)
        await db.flush()
        
        log.info("Job successfully created in database", job_id=str(new_job.id))
        
        # TODO: reddis
        
        return new_job

    except IntegrityError: #Idempotency
        await db.rollback() 
        log.warning("Idempotency key collision detected. Fetching existing job.")

        query = select(Job).where(Job.idempotency_key == x_idempotency_key)
        result = await db.execute(query)
        existing_job = result.scalars().first()

        if not existing_job:
            log.error("Integrity error occurred but job could not be retrieved")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database integrity error"
            )

        log.info(
            "Returning existing job data", 
            job_id=str(existing_job.id), 
            job_status=existing_job.status
        )
        
        return existing_job
        