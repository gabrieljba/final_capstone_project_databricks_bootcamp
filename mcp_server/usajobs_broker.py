"""
USAJobs Search API adapter.

USAJobs docs: https://developer.usajobs.gov/api-reference/get-api-search
Endpoint:     GET https://data.usajobs.gov/api/search
Auth:         Authorization-Key + User-Agent (must be a registered email)

All HTTP calls and response parsing live in this module; MCP tools stay thin.
"""

import base64
import hashlib
import os
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

_SECRET_SCOPE = os.environ.get("USAJOBS_SECRET_SCOPE", "jobs")
_API_KEY_KEY = os.environ.get("USAJOBS_API_KEY_KEY", "usajobs-api-key")
_USER_AGENT_KEY = os.environ.get("USAJOBS_USER_AGENT_KEY", "usajobs-user-agent")

_BASE_URL = "https://data.usajobs.gov/api/search"
_HOST = "data.usajobs.gov"
_TIMEOUT = 20

_api_key: str | None = None
_user_agent: str | None = None


def _secret(key: str) -> str:
    """Fetch and base64-decode a value from the Databricks secret scope."""
    secret = _w.secrets.get_secret(scope=_SECRET_SCOPE, key=key)
    return base64.b64decode(secret.value).decode("utf-8")


def _get_creds() -> tuple[str, str]:
    """Lazy-load and cache USAJobs credentials from Databricks secrets."""
    global _api_key, _user_agent
    if _api_key is None:
        _api_key = _secret(_API_KEY_KEY)
    if _user_agent is None:
        _user_agent = _secret(_USER_AGENT_KEY)
    return _api_key, _user_agent


def _stable_id(external_id: str) -> str:
    """Generate a namespaced primary key for job_postings.id."""
    return f"usajobs_{hashlib.sha256(external_id.encode()).hexdigest()[:24]}"


def search_jobs(
    query: str,
    location: str | None = None,
    results_per_page: int = 25,
    salary_min: float | None = None,
    remote_only: bool = False,
) -> list[dict]:
    """
    Search USAJobs for federal government job postings.

    Args:
        query: Free-text keyword search.
        location: City/state to filter by (e.g. "Washington, DC").
        results_per_page: 1-500 (default 25).
        salary_min: Minimum salary filter (annual USD).
        remote_only: If True, restrict to telework-eligible positions.

    Returns:
        List of normalized job posting dicts ready for Lakebase upsert.
    """
    api_key, user_agent = _get_creds()

    headers = {
        "Host": _HOST,
        "User-Agent": user_agent,
        "Authorization-Key": api_key,
    }

    params: dict[str, Any] = {
        "Keyword": query,
        "ResultsPerPage": max(1, min(int(results_per_page), 500)),
    }
    if location:
        params["LocationName"] = location
    if salary_min:
        params["RemunerationMinimumAmount"] = int(salary_min)
    if remote_only:
        params["RemoteIndicator"] = "True"

    resp = requests.get(_BASE_URL, headers=headers, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    search_result = data.get("SearchResult", {})
    items = search_result.get("SearchResultItems", [])
    return [_normalize(item) for item in items]


def _normalize(item: dict) -> dict:
    """Normalize a USAJobs SearchResultItem into our job_postings schema."""
    descriptor = item.get("MatchedObjectDescriptor", {})
    external_id = str(item.get("MatchedObjectId", ""))

    positions = descriptor.get("PositionRemuneration", [])
    salary_min = None
    salary_max = None
    currency = "USD"
    if positions:
        p = positions[0]
        try:
            salary_min = float(p.get("MinimumRange")) if p.get("MinimumRange") else None
            salary_max = float(p.get("MaximumRange")) if p.get("MaximumRange") else None
        except (ValueError, TypeError):
            pass
        currency = p.get("Description", "USD") or "USD"

    locations = descriptor.get("PositionLocation", [])
    location_str = None
    if locations:
        location_str = locations[0].get("LocationName")

    remote = descriptor.get("TeleworkEligible", False) or False

    user_area = descriptor.get("UserArea", {}).get("Details", {})
    description = (
        user_area.get("MajorDuties")
        or descriptor.get("QualificationSummary")
        or descriptor.get("PositionSummary")
        or ""
    )
    if isinstance(description, list):
        description = "\n\n".join(str(d) for d in description if d)

    return {
        "id": _stable_id(external_id),
        "source": "usajobs",
        "external_id": external_id,
        "title": descriptor.get("PositionTitle", "").strip(),
        "company": descriptor.get("OrganizationName") or descriptor.get("DepartmentName"),
        "location": location_str,
        "remote": bool(remote),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "currency": currency if len(currency) <= 3 else "USD",
        "description": (description or "").strip(),
        "url": descriptor.get("PositionURI") or descriptor.get("ApplyURI", [None])[0] if descriptor.get("ApplyURI") else descriptor.get("PositionURI"),
        "category": (descriptor.get("JobCategory") or [{}])[0].get("Name") if descriptor.get("JobCategory") else None,
        "posted_at": descriptor.get("PublicationStartDate"),
        "payload": item,
    }
