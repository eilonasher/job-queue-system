import asyncio
import random

async def process_email_job(payload: dict) -> dict:
    wait_time = random.uniform(1, 3)
    await asyncio.sleep(wait_time)
    
    to_address = payload.get("to", "unknown@example.com")
    return {
        "status": "sent",
        "message_id": f"msg_{random.randint(100000, 999999)}",
        "to": to_address,
        "duration_seconds": round(wait_time, 2)
    }
