"""
Adzuna Jobs API adapter.

Adzuna docs: https://developer.adzuna.com/docs/search
Endpoint:    GET https://api.adzuna.com/v1/api/jobs/{country}/search/{page}
Auth:        app_id + app_key as query parameters

All HTTP calls and response parsing live in this module; MCP tools stay thin.
"""

import base64
import hashlib
import os
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

_SECRET_SCOPE = os.environ.get("ADZUNA_SECRET_SCOPE", "jobs")
_APP_ID_KEY = os.environ.get("ADZUNA_APP_ID_KEY", "adzuna-app-id")
_APP_KEY_KEY = os.environ.get("ADZUNA_APP_KEY_KEY", "adzuna-app-key")

_BASE_URL = "https://api.adzuna.com/v1/api/jobs"
_TIMEOUT = 20

_app_id: str | None = None
_app_key: str | None = None


def _secret(key: str) -> str:
    """Fetch and base64-decode a value from the Databricks secret scope."""
    secret = _w.secrets.get_secret(scope=_SECRET_SCOPE, key=key)
    return base64.b64decode(secret.value).decode("utf-8")


def _get_creds() -> tuple[str, str]:
    """Lazy-load and cache Adzuna credentials from Databricks secrets."""
    global _app_id, _app_key
    if _app_id is None:
        _app_id = _secret(_APP_ID_KEY)
    if _app_key is None:
        _app_key = _secret(_APP_KEY_KEY)
    return _app_id, _app_key


def _stable_id(external_id: str) -> str:
    """Generate a namespaced primary key for job_postings.id."""
    return f"adzuna_{hashlib.sha256(external_id.encode()).hexdigest()[:24]}"


def search_jobs(
    query: str,
    location: str | None = None,
    country: str = "us",
    results_per_page: int = 20,
    salary_min: float | None = None,
    max_days_old: int | None = 30,
) -> list[dict]:
    """
    Search Adzuna for jobs matching a query.

    Args:
        query: Free-text search (title + description).
        location: City or region (e.g. "Chicago"). None = country-wide.
        country: 2-letter country code (default "us").
        results_per_page: 1-50 (default 20).
        salary_min: Minimum salary filter (annual, in local currency).
        max_days_old: Only return postings newer than N days (default 30).

    Returns:
        List of normalized job posting dicts ready for Lakebase upsert.
    """
    app_id, app_key = _get_creds()
    page = 1
    url = f"{_BASE_URL}/{country}/search/{page}"

    params: dict[str, Any] = {
        "app_id": app_id,
        "app_key": app_key,
        "what": query,
        "results_per_page": max(1, min(int(results_per_page), 50)),
        "content-type": "application/json",
    }
    if location:
        params["where"] = location
    if salary_min:
        params["salary_min"] = int(salary_min)
    if max_days_old:
        params["max_days_old"] = int(max_days_old)

    resp = requests.get(url, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    return [_normalize(r) for r in results]


def _normalize(raw: dict) -> dict:
    """Normalize an Adzuna result into our job_postings schema."""
    external_id = str(raw.get("id", ""))
    return {
        "id": _stable_id(external_id),
        "source": "adzuna",
        "external_id": external_id,
        "title": raw.get("title", "").strip(),
        "company": (raw.get("company") or {}).get("display_name"),
        "location": (raw.get("location") or {}).get("display_name"),
        "remote": _guess_remote(raw),
        "salary_min": raw.get("salary_min"),
        "salary_max": raw.get("salary_max"),
        "currency": "USD",  # Adzuna omits currency; assume USD for country=us
        "description": raw.get("description", "").strip(),
        "url": raw.get("redirect_url"),
        "category": (raw.get("category") or {}).get("label"),
        "posted_at": raw.get("created"),
        "payload": raw,
    }


def _guess_remote(raw: dict) -> bool:
    """Adzuna has no explicit remote flag; infer from title/description."""
    text = f"{raw.get('title', '')} {raw.get('description', '')}".lower()
    return any(kw in text for kw in ("remote", "work from home", "wfh", "telecommute"))
