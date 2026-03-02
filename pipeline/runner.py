"""
Pipeline Runner — SPCS SQL CI/CD Service
FastAPI app that validates and deploys SQL migration files using the SPCS
service OAuth token. Intended to run as a private SPCS service (not public).
"""

import os
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import snowflake.connector
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SERVICE_TOKEN_PATH = Path("/snowflake/session/token")

SNOWFLAKE_ACCOUNT  = os.getenv("SNOWFLAKE_ACCOUNT", "MLWWZGB-YR87884")
SNOWFLAKE_HOST     = os.getenv("SNOWFLAKE_HOST", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DB", "POC_SPCS_DB")
SNOWFLAKE_SCHEMA   = os.getenv("SNOWFLAKE_SCHEMA", "POC_SPCS_SCHEMA")
AUDIT_TABLE        = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PIPELINE_AUDIT_LOG"

app = FastAPI(title="SPCS SQL Pipeline Runner", version="1.0.0")


# ---------------------------------------------------------------------------
# Snowflake connection (service token only — no caller context needed here)
# ---------------------------------------------------------------------------

def _read_service_token() -> str:
    if not SERVICE_TOKEN_PATH.exists():
        raise RuntimeError("Service token not found at /snowflake/session/token")
    return SERVICE_TOKEN_PATH.read_text(encoding="utf-8").strip()


def _get_connection():
    token = _read_service_token()
    if not SNOWFLAKE_HOST:
        raise RuntimeError("SNOWFLAKE_HOST env var is not set")
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        host=SNOWFLAKE_HOST,
        authenticator="oauth",
        token=token,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def _write_audit(
    migration_file: str,
    status: str,
    sql_content: str,
    git_sha: str,
    error_message: Optional[str] = None,
):
    try:
        conn = _get_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
                (migration_file, status, executed_by, sql_content, error_message, git_sha)
            SELECT
                %s, %s, CURRENT_USER(), %s, %s, %s
            """,
            (migration_file, status, sql_content, error_message or "", git_sha),
        )
        cur.close()
        conn.close()
    except Exception as exc:
        # Audit failure is non-fatal — log and continue
        print(f"[WARN] Could not write audit log: {exc}")


def _last_audit_entry(migration_file: str) -> Optional[dict]:
    try:
        conn = _get_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, migration_file, status, executed_by, executed_at,
                   error_message, git_sha
            FROM {AUDIT_TABLE}
            WHERE migration_file = %s
            ORDER BY executed_at DESC
            LIMIT 1
            """,
            (migration_file,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            cols = ["id", "migration_file", "status", "executed_by",
                    "executed_at", "error_message", "git_sha"]
            entry = dict(zip(cols, row))
            # Make executed_at JSON-serialisable
            if hasattr(entry["executed_at"], "isoformat"):
                entry["executed_at"] = entry["executed_at"].isoformat()
            return entry
    except Exception as exc:
        print(f"[WARN] Could not read audit log: {exc}")
    return None


# ---------------------------------------------------------------------------
# SQL validation helper
# ---------------------------------------------------------------------------

def _split_statements(sql: str) -> list[str]:
    """Split on semicolons, skip empty lines."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def _validate_sql(sql_content: str) -> list[str]:
    """
    Wrap each statement with EXPLAIN to catch syntax errors without executing.
    Returns a list of error strings (empty list = all OK).
    """
    errors = []
    conn = _get_connection()
    cur = conn.cursor()
    for stmt in _split_statements(sql_content):
        explain = f"EXPLAIN {stmt}"
        try:
            cur.execute(explain)
        except Exception as exc:
            errors.append(str(exc))
    cur.close()
    conn.close()
    return errors


def _execute_sql(sql_content: str) -> list[str]:
    """
    Execute each statement sequentially.
    Returns list of error strings (empty = all succeeded).
    """
    errors = []
    conn = _get_connection()
    cur = conn.cursor()
    for stmt in _split_statements(sql_content):
        try:
            cur.execute(stmt)
        except Exception as exc:
            errors.append(f"Statement failed: {stmt[:120]}... — {exc}")
            break  # stop on first error
    cur.close()
    conn.close()
    return errors


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class MigrationRequest(BaseModel):
    migration_file: str          # e.g. "20260301_120000_add_index.sql"
    sql_content: str             # full SQL text
    git_sha: str = ""            # commit SHA for traceability


class ValidationResponse(BaseModel):
    migration_file: str
    valid: bool
    errors: list[str]


class DeployResponse(BaseModel):
    migration_file: str
    deployed: bool
    errors: list[str]
    audit_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Readiness probe for SPCS."""
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.post("/validate", response_model=ValidationResponse)
def validate(req: MigrationRequest):
    """
    Validate SQL syntax using EXPLAIN — does NOT execute the statements.
    Returns 200 with valid=True on success, valid=False on syntax errors.
    Raises 500 on connection failure.
    """
    try:
        errors = _validate_sql(req.sql_content)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    status = "VALIDATED" if not errors else "FAILED"
    _write_audit(
        migration_file=req.migration_file,
        status=status,
        sql_content=req.sql_content,
        git_sha=req.git_sha,
        error_message="; ".join(errors) if errors else None,
    )
    return ValidationResponse(
        migration_file=req.migration_file,
        valid=not errors,
        errors=errors,
    )


@app.post("/deploy", response_model=DeployResponse)
def deploy(req: MigrationRequest):
    """
    Execute the SQL migration. Validates first, then runs.
    Returns 200 with deployed=True on success, deployed=False on failure.
    Raises 500 on connection failure.
    """
    # Validate before executing
    try:
        val_errors = _validate_sql(req.sql_content)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if val_errors:
        _write_audit(
            migration_file=req.migration_file,
            status="FAILED",
            sql_content=req.sql_content,
            git_sha=req.git_sha,
            error_message="Validation failed: " + "; ".join(val_errors),
        )
        return DeployResponse(
            migration_file=req.migration_file,
            deployed=False,
            errors=val_errors,
        )

    # Execute
    try:
        exec_errors = _execute_sql(req.sql_content)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    status = "DEPLOYED" if not exec_errors else "FAILED"
    _write_audit(
        migration_file=req.migration_file,
        status=status,
        sql_content=req.sql_content,
        git_sha=req.git_sha,
        error_message="; ".join(exec_errors) if exec_errors else None,
    )
    return DeployResponse(
        migration_file=req.migration_file,
        deployed=not exec_errors,
        errors=exec_errors,
    )


@app.get("/audit/{migration_file:path}")
def get_audit(migration_file: str):
    """
    Fetch the latest audit log entry for a given migration file name.
    Used by GitHub Actions to commit the result back to git.
    """
    entry = _last_audit_entry(migration_file)
    if not entry:
        raise HTTPException(status_code=404, detail="No audit entry found")
    return entry
