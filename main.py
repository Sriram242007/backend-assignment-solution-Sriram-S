import asyncio
import hashlib
import io
import json
import os
import re
import sqlite3
import time
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import aiohttp
from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse


# ============================================================
# Configuration
# ============================================================

DB_PATH = os.getenv("DB_PATH", "backend_jobs.db")
PROCESSOR_URL = os.getenv(
    "PROCESSOR_URL",
    "http://127.0.0.1:8001/v1/process",
)
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "10"))
WORKER_POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "0.10"))
CLAIM_LEASE_SECONDS = int(os.getenv("CLAIM_LEASE_SECONDS", "180"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "125"))
BACKEND_MODE = os.getenv("BACKEND_MODE", "api").lower()
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")

MAX_COMPRESSED_BYTES = 64 * 1024 * 1024
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_RECORDS = 100_000
MAX_LINE_BYTES = 65_536
MAX_FILES = 1_000

JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
RUN_ID_RE = JOB_ID_RE
RECORD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,119}\.jsonl$")


# ============================================================
# Database
# ============================================================

def utc_now() -> float:
    return time.time()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = connect_db()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                archive_sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                ingestion_complete INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_run_id_idempotency
            ON jobs(run_id, idempotency_key);

            CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                filename TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'RUNNING',
                UNIQUE(job_id, filename)
            );

            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                task_db_id INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                record_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                work_ms INTEGER NOT NULL,
                timeout_ms INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                value_json TEXT,
                receipt TEXT,
                error_code TEXT,
                claimed_by TEXT,
                claimed_at REAL,
                UNIQUE(task_db_id, record_id)
            );

            CREATE INDEX IF NOT EXISTS ix_records_pending
            ON records(status, claimed_at);

            CREATE INDEX IF NOT EXISTS ix_records_job
            ON records(job_id);
            """
        )
    finally:
        conn.close()


# ============================================================
# JSON / ZIP validation
# ============================================================

class ValidationError(Exception):
    pass


def reject_constant(value: str):
    raise ValueError(f"non-finite JSON value: {value}")


def duplicate_key_pairs(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"duplicate JSON object key: {key}")
        obj[key] = value
    return obj


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def validate_record(obj: Any) -> dict:
    if not isinstance(obj, dict):
        raise ValidationError("record must be a JSON object")

    allowed = {
        "record_id",
        "payload",
        "work_ms",
        "timeout_ms",
        "max_attempts",
    }

    if set(obj) - allowed:
        raise ValidationError("unknown record field")

    required = {"record_id", "payload", "work_ms", "timeout_ms"}
    if not required.issubset(obj):
        raise ValidationError("missing required record field")

    record_id = obj["record_id"]
    payload = obj["payload"]
    work_ms = obj["work_ms"]
    timeout_ms = obj["timeout_ms"]
    max_attempts = obj.get("max_attempts", 3)

    if not isinstance(record_id, str) or not RECORD_ID_RE.fullmatch(record_id):
        raise ValidationError("invalid record_id")

    if not isinstance(payload, dict):
        raise ValidationError("payload must be an object")

    # bool is an int subclass, so explicitly reject it.
    if type(work_ms) is not int or not 25 <= work_ms <= 60000:
        raise ValidationError("invalid work_ms")

    if type(timeout_ms) is not int or not 25 <= timeout_ms <= 120000:
        raise ValidationError("invalid timeout_ms")

    if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
        raise ValidationError("invalid max_attempts")

    compact_payload = canonical_json(payload)
    if len(compact_payload.encode("utf-8")) > 16_384:
        raise ValidationError("payload exceeds 16384 UTF-8 bytes")

    return {
        "record_id": record_id,
        "payload": payload,
        "work_ms": work_ms,
        "timeout_ms": timeout_ms,
        "max_attempts": max_attempts,
    }


def validate_zip(data: bytes) -> list[tuple[str, list[dict]]]:
    if len(data) > MAX_COMPRESSED_BYTES:
        raise ValidationError("compressed archive exceeds 64 MiB")

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise ValidationError("invalid ZIP archive") from exc

    infos = zf.infolist()

    if not 1 <= len(infos) <= MAX_FILES:
        raise ValidationError("archive must contain 1-1000 files")

    names = set()
    expanded_total = 0
    total_records = 0
    result = []

    try:
        for info in infos:
            name = info.filename

            if name in names:
                raise ValidationError("duplicate filename")
            names.add(name)

            # Root-level JSONL only.
            if not TASK_ID_RE.fullmatch(name):
                raise ValidationError(f"invalid archive filename: {name}")

            if "/" in name or "\\" in name:
                raise ValidationError("nested paths are not allowed")

            if info.is_dir():
                raise ValidationError("directory entries are not allowed")

            # ZIP Unix symlink detection.
            unix_type = (info.external_attr >> 16) & 0o170000
            if unix_type == 0o120000:
                raise ValidationError("symlinks are not allowed")

            if info.flag_bits & 0x1:
                raise ValidationError("encrypted archives are not allowed")

            if info.compress_type not in (
                zipfile.ZIP_STORED,
                zipfile.ZIP_DEFLATED,
            ):
                raise ValidationError("unsupported compression method")

            expanded_total += info.file_size
            if expanded_total > MAX_EXPANDED_BYTES:
                raise ValidationError("expanded archive exceeds 256 MiB")

            file_records = []

            with zf.open(info, "r") as f:
                while True:
                    raw_line = f.readline(MAX_LINE_BYTES + 1)
                    if not raw_line:
                        break

                    if len(raw_line) > MAX_LINE_BYTES:
                        raise ValidationError("JSONL line exceeds 65536 bytes")

                    if raw_line in (b"\n", b"\r\n"):
                        continue

                    try:
                        line = raw_line.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ValidationError("JSONL is not valid UTF-8") from exc

                    try:
                        obj = json.loads(
                            line,
                            object_pairs_hook=duplicate_key_pairs,
                            parse_constant=reject_constant,
                        )
                    except (json.JSONDecodeError, ValueError) as exc:
                        raise ValidationError("invalid JSONL record") from exc

                    record = validate_record(obj)
                    file_records.append(record)

                    total_records += 1
                    if total_records > MAX_RECORDS:
                        raise ValidationError("archive exceeds 100000 records")

            if not file_records:
                raise ValidationError(f"{name} contains no records")

            result.append((name, file_records))

    finally:
        zf.close()

    return result


# ============================================================
# Result / status helpers
# ============================================================

def derive_job_status(conn: sqlite3.Connection, job_id: str) -> str:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status='RUNNING' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending
        FROM records
        WHERE job_id=?
        """,
        (job_id,),
    ).fetchone()

    total = row["total"] or 0
    running = row["running"] or 0
    pending = row["pending"] or 0
    failed = row["failed"] or 0

    if total == 0 or running or pending:
        status = "RUNNING"
    elif failed:
        status = "FAILED"
    else:
        status = "SUCCEEDED"

    conn.execute(
        "UPDATE jobs SET status=? WHERE job_id=?",
        (status, job_id),
    )

    return status


def progress_for_job(conn: sqlite3.Connection, job_id: str) -> dict:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status='RUNNING' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending
        FROM records
        WHERE job_id=?
        """,
        (job_id,),
    ).fetchone()

    files_total = conn.execute(
        "SELECT COUNT(*) AS c FROM tasks WHERE job_id=?",
        (job_id,),
    ).fetchone()["c"]

    files_terminal = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM tasks t
        WHERE t.job_id=?
        AND NOT EXISTS (
            SELECT 1 FROM records r
            WHERE r.task_db_id=t.task_id
            AND r.status IN ('PENDING','RUNNING')
        )
        """,
        (job_id,),
    ).fetchone()["c"]

    return {
        "records_total": row["total"] or 0,
        "records_succeeded": row["succeeded"] or 0,
        "records_failed": row["failed"] or 0,
        "records_running": row["running"] or 0,
        "records_pending": row["pending"] or 0,
        "files_total": files_total,
        "files_terminal": files_terminal,
    }


def refresh_job(conn: sqlite3.Connection, job_id: str) -> str:
    return derive_job_status(conn, job_id)


# ============================================================
# FastAPI API
# ============================================================

app = FastAPI(title="CIT Backend Evaluation")


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/jobs")
async def create_job(
    file: UploadFile = File(...),
    x_run_id: str = Header(..., alias="X-Run-ID"),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    if not RUN_ID_RE.fullmatch(x_run_id):
        raise HTTPException(status_code=422, detail="Invalid X-Run-ID")

    if not idempotency_key or len(idempotency_key) > 128:
        raise HTTPException(status_code=422, detail="Invalid Idempotency-Key")

    data = await file.read()

    try:
        tasks = validate_zip(data)
    except ValidationError as exc:
        status = 413 if "exceeds" in str(exc) else 422
        raise HTTPException(status_code=status, detail=str(exc))

    archive_hash = hashlib.sha256(data).hexdigest()

    conn = connect_db()
    try:
        # Transaction protects the idempotency decision against concurrent API requests.
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            """
            SELECT job_id, archive_sha256, status
            FROM jobs
            WHERE run_id=? AND idempotency_key=?
            """,
            (x_run_id, idempotency_key),
        ).fetchone()

        if existing:
            conn.commit()

            if existing["archive_sha256"] != archive_hash:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency-Key reused with different ZIP bytes",
                )

            return JSONResponse(
                {
                    "job_id": existing["job_id"],
                    "status": existing["status"],
                },
                status_code=200,
            )

        job_id = f"job-{uuid.uuid4().hex}"

        if not JOB_ID_RE.fullmatch(job_id):
            raise HTTPException(status_code=500, detail="Failed to create job ID")

        conn.execute(
            """
            INSERT INTO jobs
            (job_id, run_id, idempotency_key, archive_sha256, status,
             ingestion_complete, created_at)
            VALUES (?, ?, ?, ?, 'QUEUED', 1, ?)
            """,
            (
                job_id,
                x_run_id,
                idempotency_key,
                archive_hash,
                utc_now(),
            ),
        )

        for filename, records in tasks:
            task_db_id = conn.execute(
                """
                INSERT INTO tasks(job_id, filename, status)
                VALUES (?, ?, 'RUNNING')
                """,
                (job_id, filename),
            ).lastrowid

            for record in records:
                conn.execute(
                    """
                    INSERT INTO records
                    (job_id, task_db_id, record_id, payload_json,
                     work_ms, timeout_ms, max_attempts, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                    """,
                    (
                        job_id,
                        task_db_id,
                        record["record_id"],
                        canonical_json(record["payload"]),
                        record["work_ms"],
                        record["timeout_ms"],
                        record["max_attempts"],
                    ),
                )

        conn.commit()

    except sqlite3.IntegrityError:
        conn.rollback()

        # Another API instance won the idempotency race.
        existing = conn.execute(
            """
            SELECT job_id, archive_sha256, status
            FROM jobs
            WHERE run_id=? AND idempotency_key=?
            """,
            (x_run_id, idempotency_key),
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=500, detail="Database error")

        if existing["archive_sha256"] != archive_hash:
            raise HTTPException(
                status_code=409,
                detail="Idempotency-Key reused with different ZIP bytes",
            )

        return JSONResponse(
            {
                "job_id": existing["job_id"],
                "status": existing["status"],
            },
            status_code=200,
        )
    finally:
        conn.close()

    return JSONResponse(
        {"job_id": job_id, "status": "QUEUED"},
        status_code=202,
    )


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    conn = connect_db()
    try:
        job = conn.execute(
            "SELECT * FROM jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        status = refresh_job(conn, job_id)
        conn.commit()

        p = progress_for_job(conn, job_id)

        return {
            "job_id": job_id,
            "status": status,
            "ingestion_complete": bool(job["ingestion_complete"]),
            "files_total": p["files_total"],
            "files_terminal": p["files_terminal"],
            "records_total": p["records_total"],
            "records_succeeded": p["records_succeeded"],
            "records_failed": p["records_failed"],
            "records_running": p["records_running"],
            "records_pending": p["records_pending"],
        }
    finally:
        conn.close()


@app.get("/jobs/{job_id}/result")
async def get_result(job_id: str):
    conn = connect_db()
    try:
        job = conn.execute(
            "SELECT * FROM jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        status = refresh_job(conn, job_id)
        conn.commit()

        if status not in ("SUCCEEDED", "FAILED"):
            raise HTTPException(status_code=409, detail="Job is not terminal")

        task_rows = conn.execute(
            """
            SELECT task_id, filename
            FROM tasks
            WHERE job_id=?
            ORDER BY filename
            """,
            (job_id,),
        ).fetchall()

        files = []
        archive_sum = 0

        for task in task_rows:
            records = conn.execute(
                """
                SELECT record_id, status, attempts, value_json, receipt, error_code
                FROM records
                WHERE task_db_id=?
                ORDER BY id
                """,
                (task["task_id"],),
            ).fetchall()

            output_records = []
            value_sum = 0
            succeeded = 0
            failed = 0

            for record in records:
                item = {
                    "record_id": record["record_id"],
                    "status": record["status"],
                    "attempts": record["attempts"],
                }

                if record["status"] == "SUCCEEDED":
                    item["value"] = json.loads(record["value_json"])
                    item["receipt"] = record["receipt"]
                    value_sum += json.loads(record["value_json"])
                    succeeded += 1
                else:
                    item["error_code"] = record["error_code"] or "ATTEMPTS_EXHAUSTED"
                    failed += 1

                output_records.append(item)

            file_status = "FAILED" if failed else "SUCCEEDED"

            files.append(
                {
                    "task_id": task["filename"],
                    "status": file_status,
                    "records_total": len(records),
                    "records_succeeded": succeeded,
                    "records_failed": failed,
                    "value_sum": value_sum,
                    "records": output_records,
                }
            )

            archive_sum += value_sum

        totals = {
            "files": len(files),
            "records": sum(f["records_total"] for f in files),
            "succeeded": sum(f["records_succeeded"] for f in files),
            "failed": sum(f["records_failed"] for f in files),
            "value_sum": archive_sum,
        }

        return {
            "job_id": job_id,
            "status": status,
            "files": files,
            "totals": totals,
        }
    finally:
        conn.close()


# ============================================================
# Worker
# ============================================================

class Worker:
    def __init__(self):
        self.worker_id = WORKER_ID
        self.semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
        self.stop_event = asyncio.Event()
        self.session: aiohttp.ClientSession | None = None

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session

            while not self.stop_event.is_set():
                self.recover_stale_records()

                record_ids = self.claim_records(
                    max_count=WORKER_CONCURRENCY
                )

                if not record_ids:
                    await asyncio.sleep(WORKER_POLL_SECONDS)
                    continue

                await asyncio.gather(
                    *(self.process_record(record_id) for record_id in record_ids),
                    return_exceptions=True,
                )

    def recover_stale_records(self):
        conn = connect_db()
        try:
            cutoff = utc_now() - CLAIM_LEASE_SECONDS

            conn.execute(
                """
                UPDATE records
                SET status='PENDING',
                    claimed_by=NULL,
                    claimed_at=NULL
                WHERE status='RUNNING'
                  AND claimed_at IS NOT NULL
                  AND claimed_at < ?
                """,
                (cutoff,),
            )
        finally:
            conn.close()

    def claim_records(self, max_count: int) -> list[int]:
        conn = connect_db()
        claimed = []

        try:
            conn.execute("BEGIN IMMEDIATE")

            rows = conn.execute(
                """
                SELECT id
                FROM records
                WHERE status='PENDING'
                ORDER BY id
                LIMIT ?
                """,
                (max_count,),
            ).fetchall()

            now = utc_now()

            for row in rows:
                updated = conn.execute(
                    """
                    UPDATE records
                    SET status='RUNNING',
                        claimed_by=?,
                        claimed_at=?
                    WHERE id=? AND status='PENDING'
                    """,
                    (self.worker_id, now, row["id"]),
                )

                if updated.rowcount == 1:
                    claimed.append(row["id"])

            conn.commit()
            return claimed

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def process_record(self, record_db_id: int):
        conn = connect_db()

        try:
            row = conn.execute(
                """
                SELECT
                    r.*,
                    t.filename,
                    j.run_id,
                    j.job_id
                FROM records r
                JOIN tasks t ON t.task_id=r.task_db_id
                JOIN jobs j ON j.job_id=r.job_id
                WHERE r.id=?
                """,
                (record_db_id,),
            ).fetchone()

            if not row:
                return

            # Admit one logical attempt immediately before downstream delivery.
            next_attempt = row["attempts"] + 1

            if next_attempt > row["max_attempts"]:
                conn.execute(
                    """
                    UPDATE records
                    SET status='FAILED',
                        error_code='ATTEMPTS_EXHAUSTED',
                        claimed_by=NULL,
                        claimed_at=NULL
                    WHERE id=?
                    """,
                    (record_db_id,),
                )
                conn.commit()
                refresh_job(conn, row["job_id"])
                conn.commit()
                return

            conn.execute(
                """
                UPDATE records
                SET attempts=?, claimed_at=?
                WHERE id=? AND status='RUNNING' AND claimed_by=?
                """,
                (
                    next_attempt,
                    utc_now(),
                    record_db_id,
                    self.worker_id,
                ),
            )
            conn.commit()

            request_body = {
                "run_id": row["run_id"],
                "job_id": row["job_id"],
                "task_id": row["filename"],
                "attempt": next_attempt,
                "record": {
                    "record_id": row["record_id"],
                    "payload": json.loads(row["payload_json"]),
                    "work_ms": row["work_ms"],
                    "timeout_ms": row["timeout_ms"],
                    "max_attempts": row["max_attempts"],
                },
            }

            try:
                async with self.session.post(
                    PROCESSOR_URL,
                    json=request_body,
                ) as response:
                    body = await response.json(content_type=None)

                    if response.status == 200:
                        if "value" not in body or "receipt" not in body:
                            await self.mark_failure_or_retry(
                                record_db_id,
                                row["job_id"],
                                next_attempt,
                                row["max_attempts"],
                                "INVALID_PROCESSOR_RESPONSE",
                            )
                            return

                        await self.mark_success(
                            record_db_id,
                            row["job_id"],
                            body,
                        )
                        return

                    # 429 means the processor explicitly says the logical
                    # attempt was NOT consumed.
                    if response.status == 429:
                        await self.mark_retry_without_consuming(
                            record_db_id,
                            row["job_id"],
                            body.get("error", "OVERLOADED"),
                        )
                        return

                    await self.mark_failure_or_retry(
                        record_db_id,
                        row["job_id"],
                        next_attempt,
                        row["max_attempts"],
                        body.get("error", "PROCESSOR_ERROR"),
                    )

            except (aiohttp.ClientError, asyncio.TimeoutError):
                # A transport failure is treated as an admitted/consumed
                # logical attempt because the downstream outcome is unknown.
                await self.mark_failure_or_retry(
                    record_db_id,
                    row["job_id"],
                    next_attempt,
                    row["max_attempts"],
                    "PROCESSOR_UNAVAILABLE",
                )

        finally:
            conn.close()

    async def mark_success(self, record_id: int, job_id: str, body: dict):
        conn = connect_db()
        try:
            conn.execute(
                """UPDATE records
                   SET status='SUCCEEDED',
                   value_json=?,
                   receipt=?,
                   error_code=NULL,
                   claimed_by=NULL,
                   claimed_at=NULL
                   WHERE id=? AND status='RUNNING'""",
                (
                     canonical_json(body.get("value")),
                     body.get("receipt"),
                     record_id,
                ),
            )
            refresh_job(conn, job_id)
            conn.commit()
        finally:
            conn.close()

    async def mark_retry_without_consuming(
        self,
        record_id: int,
        job_id: str,
        error_code: str,
    ):
        conn = connect_db()
        try:
            conn.execute(
                """
                UPDATE records
                SET status='PENDING',
                    error_code=?,
                    attempts=MAX(attempts-1, 0),
                    claimed_by=NULL,
                    claimed_at=NULL
                WHERE id=? AND status='RUNNING'
                """,
                (error_code, record_id),
            )
            refresh_job(conn, job_id)
            conn.commit()
        finally:
            conn.close()

    async def mark_failure_or_retry(
        self,
        record_id: int,
        job_id: str,
        attempt: int,
        max_attempts: int,
        error_code: str,
    ):
        conn = connect_db()
        try:
            if attempt >= max_attempts:
                conn.execute(
                    """
                    UPDATE records
                    SET status='FAILED',
                        error_code='ATTEMPTS_EXHAUSTED',
                        claimed_by=NULL,
                        claimed_at=NULL
                    WHERE id=? AND status='RUNNING'
                    """,
                    (record_id,),
                )
            else:
                conn.execute(
                    """
                    UPDATE records
                    SET status='PENDING',
                        error_code=?,
                        claimed_by=NULL,
                        claimed_at=NULL
                    WHERE id=? AND status='RUNNING'
                    """,
                    (error_code, record_id),
                )

            refresh_job(conn, job_id)
            conn.commit()
        finally:
            conn.close()


# ============================================================
# Worker entry point
# ============================================================

async def worker_main():
    init_db()
    worker = Worker()
    await worker.run()


if __name__ == "__main__":
    if BACKEND_MODE == "worker":
        asyncio.run(worker_main())
    else:
        import uvicorn

        init_db()
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=int(os.getenv("PORT", "8080")),
            reload=False,
        )
