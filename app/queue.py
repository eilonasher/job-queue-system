import json
import time
from redis.asyncio import Redis
import structlog

logger = structlog.get_logger()

QUEUE_PENDING = "queue:jobs:pending"
QUEUE_SCHEDULED = "queue:jobs:scheduled"

class JobQueue:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def enqueue_job(self, job_id: str, priority: int, scheduled_at_timestamp: float = None) -> bool:
        """
        push a new job to redis queue.
        if the sceduled time is in the future -> push to futured jobs queue,
         else -> push to immideate jobs queue
        """
        job_id_str = str(job_id)
        log = logger.bind(job_id=job_id_str, priority=priority)

        try:
            if scheduled_at_timestamp:
                await self.redis.zadd(QUEUE_SCHEDULED, {job_id_str: scheduled_at_timestamp})
                log.info("Job added to Redis SCHEDULED queue", run_at=scheduled_at_timestamp)
            else:
                # if there is no scheduled time -> push to immideate jobs queue
                await self.redis.zadd(QUEUE_PENDING, {job_id_str: priority})
                log.info("Job added to Redis PENDING queue")
            return True
        except Exception as e:
            log.error("Failed to enqueue job to Redis", error=str(e))
            return False

    async def move_scheduled_to_pending(self) -> int:
        """
        check if there are jobs need to be executed and push them to queue pending
        """
        now = time.time()
    
        jobs_to_move = await self.redis.zrangebyscore(QUEUE_SCHEDULED, 0, now)
        
        moved_count = 0
        for job_id in jobs_to_move:
            async with self.redis.pipeline() as pipe:
                pipe.zrem(QUEUE_SCHEDULED, job_id)
                pipe.zadd(QUEUE_PENDING, {job_id: 0}) 
                await pipe.execute()
            moved_count += 1
            
        if moved_count > 0:
            logger.info("Moved scheduled jobs to pending queue", count=moved_count)
        return moved_count
