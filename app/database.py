import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
import redis.asyncio as aioredis

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://queue_admin:secure_password_123@postgres:5432/job_queue_db"
)

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

async def get_redis():
    return redis_client
    