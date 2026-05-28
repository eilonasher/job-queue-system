# Distributed Job Queue System

A production-grade, asynchronous, and priority-based distributed job queue system. The architecture is built on top of **FastAPI**, utilizes **PostgreSQL** as a persistent state-store (Single Source of Truth), and leverages **Redis** as a high-performance in-memory infrastructure for queue management, network deduplication (Idempotency), and delayed task scheduling.

## Key Features
- **Priority Queueing:** Higher-priority jobs are automatically prioritized and processed by the workers ahead of lower-priority ones.
- **Robust Idempotency:** Implements a foolproof client-side deduplication mechanism using a unique `X-Idempotency-Key` header to block duplicate submissions.
- **Failures & Automatic Retries:** Built-in error handling inside the worker architecture featuring automatic job retries with Exponential Backoff.
- **Scheduled Jobs:** Seamless support for scheduling delayed tasks to execute at a specific point in the future.
- **Production Observability:** A dedicated `/api/v1/health` endpoint providing real-time engine stats and individual queue metrics.
- **Automated Test Suite:** Includes 6 comprehensive automated tests covering 100% of the core lifecycles and edge cases.

---

## Getting Started (Quick Start)

The entire environment is fully containerized using Docker Compose. There is no need to manually install Python, PostgreSQL, or Redis on your local machine.

### 1. Spin Up the Environment
Open your terminal (PowerShell / Bash) in the project root directory and run:
```bash
docker compose up -d --build

### 2. Run the Automated Tests
To run the 6 robust automated integration tests covering job submission, completion, cancellation, retries, priority ordering, and idempotency, execute:

docker compose exec api env PYTHONPATH=. pytest app/tests/test_api.py -v

make sure the containers are running

### 3. Interactive API Documentation (Swagger)
Once the containers are running, you can explore, test, and interact with the endpoints directly from your browser by navigating to:
http://localhost:8000/docs

### 3. How to Submit a Test Job (Example Request)
curl -X POST "http://localhost:8000/api/v1/jobs" \
     -H "Content-Type: application/json" \
     -H "X-Idempotency-Key: my-unique-test-key-12345" \
     -d '{
       "job_type": "email",
       "payload": {"to": "user@example.com", "subject": "Hello World"},
       "priority": 5,
       "scheduled_at": null
     }'

Expected Response (201 Created)

JSON
{
  "id": "ee24cc49-a511-46d4-b40b-268fcfc7dad7",
  "status": "pending",
  "job_type": "email",
  "payload": {"to": "user@example.com", "subject": "Hello World"},
  "priority": 5,
  "created_at": "2026-05-27T18:24:58.123456"
}

