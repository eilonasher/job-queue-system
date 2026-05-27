import asyncio
import random

async def process_webhook_job(payload: dict) -> dict:
    
    await asyncio.sleep(random.uniform(1, 2))
    
    if random.random() < 0.20:
        raise RuntimeError("Webhook target server responded with 504 Gateway Timeout")
        
    return {
        "status": "delivered",
        "http_code": 200,
        "url": payload.get("url", "https://example.com/webhook")
    }
