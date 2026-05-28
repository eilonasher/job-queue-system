import pytest
import uuid
import asyncio
from datetime import datetime
from httpx import AsyncClient
from sqlalchemy import delete

from app.main import app
from app.schemas import JobStatus, JobType
from app.database import AsyncSessionLocal, redis_client
from app.models import Job
from app.queue import QUEUE_PENDING, JobQueue

BASE_URL = "http://test"

@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
async def cleanup_each_test():
    """
    setup environment
    """
    async with AsyncSessionLocal() as db:
        await db.execute(delete(Job))
        await db.commit()
    await redis_client.flushdb()
    yield #run test
    async with AsyncSessionLocal() as db:
        await db.execute(delete(Job))
        await db.commit()
    await redis_client.flushdb()


# 1. Job submission and retrieval
@pytest.mark.asyncio
async def test_job_submission_and_retrieval():
    """
    Test steps
    1. submit job
    2. get its data and verify is identical 
    """
    unique_key = f"key_submit_{uuid.uuid4()}"
    test_payload = {"to": "test@test.com"}
    payload = {
        "job_type": JobType.EMAIL.value, 
        "payload": test_payload, 
        "priority": 3,
        "scheduled_at": None
    }
    headers = {"X-Idempotency-Key": unique_key}

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        submit_res = await ac.post("/api/v1/jobs", json=payload, headers=headers)
        assert submit_res.status_code == 201, f"Failed: {submit_res.text}"
        job_id = submit_res.json()["id"]

        get_res = await ac.get(f"/api/v1/jobs/{job_id}")
        assert get_res.status_code == 200
        
        data = get_res.json()
        assert data["id"] == job_id
        assert data["job_type"] == JobType.EMAIL.value

# 2. Job completion flow
@pytest.mark.asyncio
async def test_job_completion_flow():
    """
    lifecycle of a job (happy flow)
    test steps:
    1. submit job
    2. update it as completed
    3. see if the job status and data were updated accordingly
    """
    job_id = str(uuid.uuid4())
    unique_key = f"key_comp_{uuid.uuid4()}"
    
    async with AsyncSessionLocal() as db:
        test_job = Job(
            id=uuid.UUID(job_id),
            job_type=JobType.REPORT.value,
            payload={"format": "pdf"},
            priority=1,
            idempotency_key=unique_key,
            status=JobStatus.PROCESSING.value,
            progress=50
        )
        db.add(test_job)
        await db.commit()

    async with AsyncSessionLocal() as db:
        db_job = await db.get(Job, uuid.UUID(job_id))
        db_job.status = JobStatus.COMPLETED.value
        db_job.result = {"download_url": "http://s3.com/file.pdf"}
        db_job.progress = 100
        db_job.completed_at = datetime.utcnow()
        await db.commit()

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(f"/api/v1/jobs/{job_id}")
        
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == JobStatus.COMPLETED.value
    assert data["progress"] == 100
    assert data["result"] == {"download_url": "http://s3.com/file.pdf"}

# 3. Job failure and retry
@pytest.mark.asyncio
async def test_job_failure_and_retry():
    """
    test steps:
    1. submit job
    2. call handle job failure - to testify as if the job failed
    3. verify that the backoff worked
    """
    from app.worker import handle_job_failure
    
    async with AsyncSessionLocal() as db:
        test_job = Job(
            id=uuid.uuid4(),
            job_type=JobType.WEBHOOK.value,
            payload={"url": "http://fail.com"},
            priority=5,
            idempotency_key=f"key_fail_{uuid.uuid4()}",
            status=JobStatus.PROCESSING.value,
            max_attempts=3,
            current_attempts=0
        )
        db.add(test_job)
        await db.commit()
        
        await handle_job_failure(test_job, "504 Gateway Timeout", db)
        job_id = test_job.id

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(f"/api/v1/jobs/{job_id}")
        
    data = response.json()
    assert data["status"] == JobStatus.SCHEDULED.value
    assert data["current_attempts"] == 1


# 4. Cancellation
@pytest.mark.asyncio
async def test_job_cancellation():
    """
    test steps:
    1. create job
    2. cancel it using api
    3. verify it canceled and not in the db "pending" queue
    """
    unique_key = f"key_canc_{uuid.uuid4()}"
    payload = {
        "job_type": JobType.EMAIL.value, 
        "payload": {}, 
        "priority": 5,
        "scheduled_at": None
    }
    headers = {"X-Idempotency-Key": unique_key}

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        create_res = await ac.post("/api/v1/jobs", json=payload, headers=headers)
        assert create_res.status_code == 201, f"Failed: {create_res.text}"
        job_id = create_res.json()["id"]

        cancel_res = await ac.post(f"/api/v1/jobs/{job_id}/cancel")
        assert cancel_res.status_code == 200

        get_res = await ac.get(f"/api/v1/jobs/{job_id}")
        assert get_res.json()["status"] == JobStatus.CANCELLED.value

    redis_rank = await redis_client.zrank(QUEUE_PENDING, str(job_id))
    assert redis_rank is None


# 5. Idempotency
@pytest.mark.asyncio
async def test_idempotency_key_deduplication():
    """
    test steps:
    1. submit two identical job id
    2. check if the res from db is identical (same job)
    """
    unique_key = f"key_idem_{uuid.uuid4()}"
    payload = {
        "job_type": JobType.BATCH.value, 
        "payload": {"task": "process_data"},  # תוקן: שדה payload חובה
        "priority": 2,
        "scheduled_at": None
    }
    headers = {"X-Idempotency-Key": unique_key}

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        res1 = await ac.post("/api/v1/jobs", json=payload, headers=headers)
        assert res1.status_code == 201, f"First request failed: {res1.text}"
        job1_id = res1.json()["id"]

        res2 = await ac.post("/api/v1/jobs", json=payload, headers=headers)
        assert res2.status_code in [200, 201]
        job2_id = res2.json()["id"]

    assert job1_id == job2_id


# 6. Priority ordering
@pytest.mark.asyncio
async def test_priority_ordering_in_queue():
    """
    test steps:
    1. submit 3 jobs with prorities 1, 10, 5
    2. verify that the job with priority 10 will be first
    """
    queue = JobQueue(redis_client)
    
    job_low = str(uuid.uuid4())
    job_high = str(uuid.uuid4())
    job_medium = str(uuid.uuid4())
    
    await queue.enqueue_job(job_id=job_low, priority=1)
    await queue.enqueue_job(job_id=job_high, priority=10) 
    await queue.enqueue_job(job_id=job_medium, priority=5)

    next_jobs = await redis_client.zrevrange(QUEUE_PENDING, 0, 0)
    
    assert len(next_jobs) == 1
    assert next_jobs[0] == job_high
    