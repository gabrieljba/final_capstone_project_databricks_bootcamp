"""
One-time secret setup script for the AI Job Hunting Copilot capstone.

Run this in a Databricks notebook (NOT locally). It writes all required
secrets to the Databricks secret scopes used by the MCP server and
dashboard apps. Values are base64-encoded (same pattern as Day 3).

Usage (in a Databricks notebook cell):

    # Fill in the plaintext values below, then run this cell:
    LAKEBASE_URL       = "postgresql://user:pass@host:5432/databricks_postgres?sslmode=require"
    ADZUNA_APP_ID      = "your_adzuna_app_id"
    ADZUNA_APP_KEY     = "your_adzuna_app_key"
    USAJOBS_API_KEY    = "your_usajobs_api_key"
    USAJOBS_USER_AGENT = "your.email@example.com"  # USAJobs requires a registered email as User-Agent

    exec(open("/Workspace/path/to/setup_secrets.py").read())

Alternatively, run each `databricks secrets put-secret` command in a
terminal after base64-encoding your values.
"""

import base64
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _b64(value: str) -> str:
    """Base64-encode a string (same format the broker modules decode from)."""
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


def put_secret(scope: str, key: str, plaintext_value: str) -> None:
    """Create the scope if missing, then upsert the secret (base64-encoded)."""
    try:
        w.secrets.create_scope(scope=scope)
        print(f"[+] Created secret scope: {scope}")
    except Exception:
        # Scope already exists - ignore
        pass

    w.secrets.put_secret(
        scope=scope,
        key=key,
        string_value=_b64(plaintext_value),
    )
    print(f"[+] Set secret: {scope}/{key}")


# ---------------------------------------------------------------------------
# Fill these in BEFORE running (leave as empty strings to skip a secret)
# ---------------------------------------------------------------------------
LAKEBASE_URL: str = ""        # e.g. "postgresql://role:pass@host:5432/databricks_postgres?sslmode=require"
ADZUNA_APP_ID: str = ""       # From https://developer.adzuna.com/
ADZUNA_APP_KEY: str = ""      # From https://developer.adzuna.com/
USAJOBS_API_KEY: str = ""     # From https://developer.usajobs.gov/apirequest/
USAJOBS_USER_AGENT: str = ""  # Your registered email (USAJobs requires this as User-Agent)


if __name__ == "__main__" or True:
    if LAKEBASE_URL:
        put_secret("database", "lakebase-url", LAKEBASE_URL)
    if ADZUNA_APP_ID:
        put_secret("jobs", "adzuna-app-id", ADZUNA_APP_ID)
    if ADZUNA_APP_KEY:
        put_secret("jobs", "adzuna-app-key", ADZUNA_APP_KEY)
    if USAJOBS_API_KEY:
        put_secret("jobs", "usajobs-api-key", USAJOBS_API_KEY)
    if USAJOBS_USER_AGENT:
        put_secret("jobs", "usajobs-user-agent", USAJOBS_USER_AGENT)

    print("\n[✓] Done. Verify with:")
    print("    databricks secrets list-secrets database")
    print("    databricks secrets list-secrets jobs")
