# Backend Job Processing System

A production-grade backend service for processing ZIP archives containing JSONL files through an external processor API. Implements robust job management, worker processing, idempotency, and comprehensive error handling.


### Prerequisites
- Python 3.8+
- Windows/Linux/macOS

### Setup

1. **Clone/download the repository**
```bash
cd backend-assignment
```

2. **Create virtual environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Initialize database**
```bash
python main.py  # This creates database automatically
```

## Running the Backend

### Configuration

Copy and customize `.env.example` to `.env`:
```bash
copy .env.example .env  # Windows
cp .env.example .env    # Linux/macOS
```

### Start API Server

```bash
# Terminal 1: Start API on port 8080
python main.py
```

### Start Workers

```bash
# Terminal 2: Start Worker 1
set BACKEND_MODE=worker
set WORKER_ID=worker-1
python main.py

# Terminal 3: Start Worker 2
set BACKEND_MODE=worker
set WORKER_ID=worker-2
python main.py
```
### Start Processor (from assignment repo)

```bash
# Terminal 4: Start the processor
cd path/to/cit-backend-eval-2026
./run-processor.cmd  # Windows
./run-processor.sh   # Linux/macOS
```
Expected output:

Processor running on http://0.0.0.0:8001

## API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```

Response:
```json
{"status": "ok"}
```

### Create Job (Upload ZIP)
```bash
curl -X POST \
  -H "X-Run-ID: test-run-001" \
  -H "Idempotency-Key: upload-001" \
  -F "file=@test.zip" \
  http://localhost:8080/jobs
```

Response:
```json
{
  "job_id": "job-abc123...",
  "status": "QUEUED"
}
```

### Get Job Progress
```bash
curl http://localhost:8080/jobs/job-abc123...
```

Response:
```json
{
  "job_id": "job-abc123...",
  "status": "RUNNING",
  "ingestion_complete": true,
  "files_total": 2,
  "files_terminal": 0,
  "records_total": 100,
  "records_succeeded": 45,
  "records_failed": 2,
  "records_running": 20,
  "records_pending": 33
}
```

### Get Final Results
```bash
curl http://localhost:8080/jobs/job-abc123.../result
```

Response (when job is terminal):
```json
{
  "job_id": "job-abc123...",
  "status": "SUCCEEDED",
  "files": [
    {
      "task_id": "users.jsonl",
      "status": "SUCCEEDED",
      "records_total": 50,
      "records_succeeded": 48,
      "records_failed": 2,
      "value_sum": 5000,
      "records": [
        {
          "record_id": "user-1",
          "status": "SUCCEEDED",
          "attempts": 1,
          "value": 100,
          "receipt": "receipt-123"
        }
      ]
    }
  ],
  "totals": {
    "files": 1,
    "records": 50,
    "succeeded": 48,
    "failed": 2,
    "value_sum": 5000
  }
}
```

## Testing

### Run Test Script
```bash
python test_backend.py
```