"""
RemoteOK API adapter.

RemoteOK docs: https://remoteok.com/api  (no API key required)
Endpoint:      GET https://remoteok.com/api
Auth:          None (requires User-Agent header).

Returns a flat list of remote-only job postings. We filter client-side
by keyword since the API has no search parameters.
"""

import hashlib
from typing import Any

import requests

_BASE_URL = "https://remoteok.com/api"
_USER_AGENT = "AI-Job-Hunting-Copilot/1.0 (educational project)"
_TIMEOUT = 20


def _stable_id(external_id: str) -> str:
    """Generate a namespaced primary key for job_postings.id."""
    return f"remoteok_{hashlib.sha256(external_id.encode()).hexdigest()[:24]}"


def search_jobs(query: str, limit: int = 20, salary_min: float | None = None) -> list[dict]:
    """
    Search RemoteOK for remote jobs. Filtering is client-side.

    Args:
        query: Free-text keyword to filter title/description/tags.
        limit: Max number of results (default 20).
        salary_min: Minimum salary filter (annual USD).

    Returns:
        List of normalized job posting dicts ready for Lakebase upsert.
    """
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    resp = requests.get(_BASE_URL, headers=headers, timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    # First element is API metadata, actual jobs start at index 1
    jobs: list[dict] = [j for j in data if isinstance(j, dict) and j.get("id")]

    query_lower = (query or "").lower().strip()
    matches: list[dict] = []
    for j in jobs:
        if len(matches) >= limit:
            break
        if query_lower and not _matches_query(j, query_lower):
            continue
        if salary_min:
            try:
                job_salary = float(j.get("salary_min") or 0)
                if job_salary < salary_min:
                    continue
            except (ValueError, TypeError):
                pass
        matches.append(_normalize(j))

    return matches


def _matches_query(job: dict, query_lower: str) -> bool:
    """Check if a job matches the query in title / description / tags."""
    haystack_parts: list[str] = [
        str(job.get("position", "")),
        str(job.get("company", "")),
        str(job.get("description", "")),
    ]
    tags = job.get("tags") or []
    if isinstance(tags, list):
        haystack_parts.extend(str(t) for t in tags)
    haystack = " ".join(haystack_parts).lower()
    return query_lower in haystack


def _normalize(raw: dict) -> dict:
    """Normalize a RemoteOK job into our job_postings schema."""
    external_id = str(raw.get("id", ""))
    description = raw.get("description", "") or ""
    # RemoteOK descriptions can contain HTML; strip crude tags
    description = _strip_html(description)

    return {
        "id": _stable_id(external_id),
        "source": "remoteok",
        "external_id": external_id,
        "title": (raw.get("position") or "").strip(),
        "company": raw.get("company"),
        "location": raw.get("location") or "Remote",
        "remote": True,
        "salary_min": _to_float(raw.get("salary_min")),
        "salary_max": _to_float(raw.get("salary_max")),
        "currency": "USD",
        "description": description.strip(),
        "url": raw.get("url") or raw.get("apply_url"),
        "category": (raw.get("tags") or [None])[0] if raw.get("tags") else None,
        "posted_at": raw.get("date"),
        "payload": raw,
    }


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None


def _strip_html(text: str) -> str:
    """Very small HTML tag stripper - avoids adding beautifulsoup as a dep."""
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
