# Design Decisions

## 1. Job Pickup Strategy

**Approach chosen:**
Atomic queue operations using Redis Sorted Sets (`ZSET`) combined with an explicit database state transition guard.

**Why:**
When running multiple worker nodes concurrently, there is a severe risk of race conditions where two workers might pull and process the same high-priority job simultaneously. By using Redis atomic primitives, once a worker processes a pop action, the job ID is instantly removed from the global `PENDING` queue, ensuring no other worker can see it. Before executing the payload, the worker executes a single transaction in PostgreSQL to update the status from `PENDING` to `PROCESSING`.

**Trade-offs:**
- *What we gained:* Guaranteed "exactly-once" pickup per execution attempt, extremely low latency via Redis brokers, and absolute thread/process isolation.
- *What we gave up:* We rely heavily on network synchronization between Redis and PostgreSQL. If a network flicker happens right between pulling from Redis and updating PostgreSQL, a transient state mismatch can occur.

---

## 2. Worker Crash Recovery

**Approach chosen:**
A passive background monitor (Reaper pattern) leveraging a lease visibility timeout tracked via automatic model timestamps (`updated_at`).

**Why:**
If a worker crashes abruptly due to hardware failure, network disconnection, or a severe Out-of-Memory (OOM) error mid-job, it cannot clean up after itself or catch the exception. An active heartbeat network can introduce heavy overhead, so a passive database scan on dead leases balances performance and reliability.

**What happens if worker crashes mid-job:**
1. The job is left stranded in the PostgreSQL database with a status of `PROCESSING`.
2. The background monitor regularly executes a query looking for jobs in `PROCESSING` whose `updated_at` timestamp is older than a predefined threshold (e.g., 5 minutes).
3. If found, the monitor revokes the stale lease. If the job's `current_attempts` count is strictly below `max_attempts`, it resets the status to `PENDING` and re-queues its ID back into Redis.
4. If it has reached `max_attempts`, it transitions the state to `FAILED` permanently with a crash signature log.

---

## 3. Priority Queue Implementation

**Approach chosen:**
Redis Sorted Sets (`ZSET`) where the element is the unique Job ID and the score is the integer priority rank.

**Why:**
Standard FIFO queues (like Redis Lists using `LPUSH`/`RPOP`) cannot natively handle dynamic priority ordering or delayed scheduling without spawning multiple complex arrays. Redis Sorted Sets maintain a self-sorting data structure with an efficient time complexity of `O(log(N))`. Workers can safely query the set using `ZREVRANGE` to instantly consume the highest priority task available.

---

## 4. Retry Backoff Strategy

**Approach chosen:**
State-driven Exponential Backoff executed via the Redis `SCHEDULED` sorted set broker.

**Why:**
Retrying failed jobs immediately can overwhelm third-party APIs or internal databases if they are experiencing transient downtimes. Pushing retries into an explicit delayed schedule gives systems time to recover.

**Timing:**
- **Attempt 1:** Immediate (or near-immediate, 5-second buffer) to catch temporary network flickers.
- **Attempt 2:** After a short delay (30 seconds) if the failure persists.
- **Attempt 3:** After a longer delay (2 minutes) to allow system cooldown.
- *Beyond max attempts:* The task transitions to `FAILED` permanently, recording the full error stack trace for developer inspection.

---

## 5. One Thing I Would Do Differently With More Time

If given more time, I would replace the passive background pollers with an active **Two-Phase Lease and Dead-Letter Queue (DLQ)** subsystem using Redis hashes or streams (like Redis Streams consumer groups with `XCLAIM`). 

Currently, our crash-recovery mechanism relies on polling a relational database (`PostgreSQL`), which could lead to performance bottlenecks under heavy production loads. Transitioning the lease tracker entirely to an In-Memory visibility timeout managed natively by Redis Streams would dramatically reduce database read/write amplification, allow sub-second crash detection, and separate operational queue state completely from historical business analytics data.