import asyncio
import random

async def process_report_job(payload: dict) -> dict:
    
    report_type = payload.get("report_type", "generic")
    logger_context = {"report_type": report_type}
    
    total_records = random.randint(5000, 50000)
    await asyncio.sleep(random.uniform(3, 5))
    
    return {
        "status": "generated",
        "report_type": report_type,
        "total_records_processed": total_records,
        "file_size_kb": round(total_records * 0.15, 2),
        "download_url": f"https://s3.company.com/reports/report_{random.randint(10000, 99999)}.xlsx"
    }
