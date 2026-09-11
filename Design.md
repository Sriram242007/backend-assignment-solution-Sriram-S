# Design Document: Backend Job Processing System

## Executive Summary

This backend processes ZIP archives containing JSONL files by:
1. Accepting jobs via POST /jobs with idempotency guarantees
2. Extracting and validating all records from JSONL files
3. Processing records through an external processor API
4. Tracking progress and aggregating results
5. Exposing progress and terminal results via REST API

## Architecture

### System Components
┌─────────────────────────────────────────┐
│ FastAPI HTTP Server │
│ POST /jobs │
│ GET /jobs/{job_id} │
│ GET /jobs/{job_id}/result │
│ GET /health │
└────────────┬────────────────────────────┘
│
┌───────┴────────┐
│ │
┌────▼─────────┐ ┌──▼──────────────┐
│ Database │ │ Async Worker │
│ (SQLite) │ │ Pool (N=10) │
└────┬─────────┘ └──┬──────────────┘
│ │
└───────┬───────┘
│
┌───────▼───────────┐
│ External Processor│
│ API (HTTP) │
└───────────────────┘

### Database Schema

**jobs** table:
- `job_id`: Unique job identifier
- `run_id`: Run context identifier
- `idempotency_key`: Deduplication key
- `archive_sha256`: Archive hash for verification
- `status`: QUEUED, RUNNING, SUCCEEDED, FAILED
- `ingestion_complete`: Always 1 (reserved for future)
- `created_at`: Timestamp

**tasks** table:
- `task_id`: Unique task identifier (database-assigned)
- `job_id`: Foreign key to jobs
- `filename`: JSONL filename
- `status`: RUNNING or COMPLETED

**records** table:
- `id`: Unique record identifier (database-assigned)
- `job_id`: Foreign key to jobs
- `task_db_id`: Foreign key to tasks
- `record_id`: Record identifier from JSONL
- `payload_json`: Canonical JSON payload
- `work_ms`: Expected processing time
- `timeout_ms`: Timeout for processor
- `max_attempts`: Maximum retry attempts
- `status`: PENDING, RUNNING, SUCCEEDED, FAILED
- `attempts`: Current attempt count
- `value_json`: Processor output value
- `receipt`: Processor receipt for idempotency
- `error_code`: Error reason if failed
- `claimed_by`: Worker ID claiming record
- `claimed_at`: Claim timestamp for stale recovery

### State Machine

#### Record State Transitions
PENDING
↓ (worker claims)
RUNNING
├→ SUCCEEDED (processor 200 OK)
├→ PENDING (processor 429 or transient error, retry)
├→ FAILED (processor error or attempts exhausted)

#### Job State Derivation

Job status derived from records:

QUEUED: Initial state
RUNNING: Any record not terminal (PENDING or RUNNING)
SUCCEEDED: All records SUCCEEDED
FAILED: At least one record FAILED

## Ingestion Flow

1. **Receive ZIP Upload** (POST /jobs)
   - Verify headers: X-Run-ID, Idempotency-Key
   - Read ZIP file into memory
   - Calculate SHA256 hash

2. **Check Idempotency**
   - Query database for (run_id, idempotency_key)
   - If exists and hash matches: Return existing job_id
   - If exists and hash differs: Return 409 conflict
   - If not exists: Proceed to ingest

3. **Validate ZIP Archive**
   - Check ZIP format validity
   - Verify file count (1-1000)
   - Verify no directory entries, symlinks, encryption
   - Verify supported compression (STORED, DEFLATED)

4. **Process JSONL Files**
   - For each JSONL file in ZIP:
     - Verify filename matches pattern (must end .jsonl, no paths)
     - Parse each line as JSON
     - Validate record structure
     - Check for duplicate JSON keys
     - Verify non-finite JSON values rejected
     - Enforce max line size (65536 bytes)

5. **Record Validation**
   - Required fields: record_id, payload, work_ms, timeout_ms
   - Optional: max_attempts (default 3, range 1-5)
   - Verify record_id format: [A-Za-z0-9][A-Za-z0-9_-]{0,63}
   - Verify payload is JSON object
   - Verify payload size ≤ 16384 bytes
   - Verify work_ms: 25-60000 ms
   - Verify timeout_ms: 25-120000 ms

6. **Store in Database**
   - Create job entry (status: QUEUED)
   - Create task entries (status: RUNNING)
   - Create record entries (status: PENDING)
   - Commit transaction atomically

## Processing Flow

### Worker Loop
1. **Recover Stale Records**
   - Find RUNNING records with claim older than CLAIM_LEASE_SECONDS
   - Reset to PENDING status (recovery from crash)

2. **Claim Work**
   - Query PENDING records (LIMIT = WORKER_CONCURRENCY)
   - Atomically update status → RUNNING, set claimed_by/claimed_at
   - Collect claimed record IDs

3. **Process in Parallel**
   - For each claimed record, spawn async task
   - Use semaphore to limit concurrent requests

4. **Repeat**
   - Sleep WORKER_POLL_SECONDS
   - Continue until stop signal

### Record Processing

1. **Pre-flight Check**
   - Fetch record from database
   - Increment attempt counter
   - If attempts > max_attempts: Mark FAILED, return

2. **Call Processor**
   - Build request with run_id, job_id, task_id, attempt, record
   - POST to PROCESSOR_URL with HTTP_TIMEOUT_SECONDS timeout

3. **Handle Response**

   **Status 200 (Success)**
   - Verify response contains "value" and "receipt"
   - Store value (JSON), receipt (string)
   - Mark record SUCCEEDED

   **Status 429 (Overloaded)**
   - Decrement attempt counter (restore)
   - Mark PENDING for retry
   - Note: Logical attempt NOT consumed

   **Status 5xx or timeout**
   - Logical attempt consumed
   - If attempts < max_attempts: Mark PENDING (retry)
   - If attempts >= max_attempts: Mark FAILED with error code

4. **Update Job Status**
   - After each record processed, refresh job status
   - Derive status from all records

## Idempotency & Durability

### Idempotency Mechanisms

1. **Request Deduplication** (POST /jobs)
   - Use (run_id, idempotency_key) as unique key
   - Archive SHA256 verification
   - Return existing job if duplicate detected

2. **Processor Receipt**
   - Processor provides unique receipt per logical attempt
   - Receipt used to prevent counting same success twice
   - Not currently persisted but available in response

### Durability

1. **Database Persistence**
   - All decisions persisted to SQLite before processor call
   - WAL mode for concurrent reads/writes
   - Foreign key constraints enforce referential integrity

2. **Worker Crash Recovery**
   - Records claimed with timestamp
   - Stale claims (>CLAIM_LEASE_SECONDS) reset to PENDING
   - Safe to retry (processor has receipt idempotency)

3. **Transactional Consistency**
   - Job creation wrapped in BEGIN IMMEDIATE
   - No partial records inserted if ZIP invalid
   - Idempotency race handled with IntegrityError

## Performance Considerations

### Concurrency
- Semaphore limits concurrent HTTP calls to WORKER_CONCURRENCY (default 10)
- Respects processor capacity constraints
- Multiple workers can run in parallel (stateless, shared database)

### Latency
- Async processing minimizes thread blocking
- Record claiming batches reduces transaction overhead
- WAL mode allows concurrent reads during writes

### Scalability
- Single SQLite instance bottleneck at ~1000+ concurrent records
- Multiple workers can be added (no distributed transaction needed)
- For extreme scale, replace SQLite with PostgreSQL

### Memory
- Streaming ZIP extraction (not loading full archive into memory)
- Async streams prevent blocking on I/O
- Max archive size: 64 MiB compressed, 256 MiB expanded

## Failure Scenarios & Recovery

### Processor Timeout
- Treated as fatal attempt (attempt count incremented)
- Retry if attempts < max_attempts
- No partial results stored

### Processor Returns 429
- Attempt count NOT incremented
- Record stays at current attempt number
- Rescheduled for retry without consuming budget

### Worker Crash
- Records in RUNNING state with stale claim → PENDING (on next worker poll)
- In-flight request lost but not counted twice
- Processor will reject duplicate receipt if somehow sent

### Database Corruption
- WAL mode reduces corruption risk
- Worst case: Delete backend_jobs.db and restart
- Job state is in archive anyway (could be re-uploaded with same key)

### Concurrent Idempotency Requests
- Both threads race for BEGIN IMMEDIATE lock
- Winner creates job, loser gets IntegrityError
- Both return same job_id (via second SELECT)

## Tradeoffs

### Chosen: SQLite vs Alternatives
- **Chosen**: SQLite with WAL mode
- **Pro**: No external dependency, simple setup
- **Con**: Doesn't scale beyond ~10-20 concurrent workers
- **Alternative**: PostgreSQL for distributed, but overkill for this assignment

### Chosen: Async Workers vs Thread Pool
- **Chosen**: Async (asyncio + aiohttp)
- **Pro**: Low overhead, handles 1000s of concurrent I/O
- **Con**: Python single-threaded (GIL), but okay for I/O-bound
- **Alternative**: Thread pool, but more memory

### Chosen: Record Claiming vs Work Queue
- **Chosen**: Poll-based claiming
- **Pro**: Simple, no message broker needed
- **Con**: Poll latency, database load
- **Alternative**: Redis queue, but adds dependency

### Chosen: Stale Recovery Timeout
- **Chosen**: CLAIM_LEASE_SECONDS = 180s
- **Pro**: Reasonable for slow processors
- **Con**: If worker crashes, 3-min delay before retry
- **Alternative**: Shorter timeout (60s) or heartbeat mechanism

## Metrics & Monitoring

### Available via GET /jobs/{job_id}
- records_total, records_succeeded, records_failed, records_running, records_pending
- files_total, files_terminal
- Derive throughput: records_succeeded / (now - created_at)

### Available via GET /jobs/{job_id}/result (when terminal)
- Per-file value_sum
- Archive-wide value_sum
- Per-record attempt count

### Not Implemented (Nice-to-Have)
- Worker heartbeat metrics
- Processor latency histogram
- Retry rate breakdown
- Error code distribution

## Testing Strategy

### Unit Tests
- ZIP validation: valid/invalid cases
- JSON parsing: edge cases (duplicates, special chars)
- Record validation: boundary conditions
- State transitions: all valid paths

### Integration Tests
- Full job ingestion + processing
- Idempotency: duplicate requests
- Retry behavior: processor failures
- Worker crash recovery

### Load Tests
- 1000+ records per job
- 10+ concurrent jobs
- Long-running workers
- Processor latency variance

## Limitations Not Solved

1. **Distributed Consensus**
   - No support for multiple machines sharing state
   - Would need distributed lock (Zookeeper, etcd)

2. **Automatic Scaling**
   - No monitoring of processor queue depth
   - Admin must manually adjust WORKER_CONCURRENCY
   - No auto-add/remove workers

3. **Real-time Notifications**
   - No WebSocket or Server-Sent Events
   - Client must poll for progress
   - Mitigated by efficient state derivation

4. **Ordered Processing**
   - No priority queue or ordering guarantees
   - Records processed in arbitrary order
   - Would need priority column + comparator

5. **Partial Restarts**
   - Cannot selectively retry failed records
   - Must re-upload entire archive
   - Mitigated by idempotency (safe re-upload)

## Deployment

### Development
```bash
# Single terminal for development
export BACKEND_MODE=api
python main.py &
export BACKEND_MODE=worker
python main.py
```

### Production
```bash
# Terminal 1: API server
uvicorn main:app --host 0.0.0.0 --port 8080 --workers 1

# Terminal 2+: Workers (N workers for N cores)
export BACKEND_MODE=worker
python main.py
```

### Docker
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
COPY .env.example .env
CMD ["python", "main.py"]
```

## Conclusion

This implementation prioritizes **correctness and simplicity** over advanced features:
- Robust idempotency and durability
- Clear state machine with recovery
- Async I/O for efficiency
- Minimal external dependencies
- Suitable for the assignment requirements

Production deployment would add: distributed locking, message queues, metrics/monitoring, auto-scaling, multi-region failover.