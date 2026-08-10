"""
AI Job Hunting Copilot - MCP Server.

Exposes ~14 tools that let a Databricks Agent Bricks agent search jobs
across three third-party APIs (Adzuna, USAJobs, RemoteOK), embed and
semantically retrieve postings, manage a per-user pipeline in Lakebase,
draft cover letters, and generate analytics. Every tool call is logged
to `agent_activity_log` so the dashboard can render a live feed.

Deploy this as its own Databricks App (mirror of the Day 3
`mcp_server/alpaca_mcp_server.py` layout). The dashboard app in
`../dashboard/` reads from the same Lakebase database and reflects
the agent's activity in near real time.

Tool inventory:
  Search / retrieval
    - search_jobs_all_sources
    - search_adzuna
    - search_usajobs
    - search_remoteok
    - semantic_search_jobs
    - get_job_details
  Writes / actions
    - upsert_user_profile
    - save_job_to_pipeline
    - update_application_stage
    - add_interview_note
    - draft_cover_letter
  Analytics / reasoning
    - explain_job_match
    - find_stale_applications
    - get_pipeline_summary

Run locally:
    python job_mcp_server.py
"""

import json
import logging
import os
from contextvars import ContextVar
from typing import Any

from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

import adzuna_broker
import embeddings
import lakebase
import remoteok_broker
import usajobs_broker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job-mcp-server")

_request_context: ContextVar[dict] = ContextVar("request_context", default={})

mcp = FastMCP("ai-job-hunting-copilot")


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Capture Databricks App identity headers on every request."""

    async def dispatch(self, request: Request, call_next):
        headers = {
            "x-forwarded-user": request.headers.get("x-forwarded-user"),
            "x-forwarded-email": request.headers.get("x-forwarded-email"),
        }
        _request_context.set(headers)
        return await call_next(request)


# ---------------------------------------------------------------------------
# Small internal helpers (not @mcp.tool - not exposed to the agent)
# ---------------------------------------------------------------------------


def _log_activity(email: str = 'gbarriosarias@gmail.com' | None, tool_name: str, params: dict, summary: str, status: str = "success") -> None:
    """Insert a row into agent_activity_log so the dashboard feed can render it."""
    try:
        lakebase.run_write(
            """
            INSERT INTO agent_activity_log (email, tool_name, params, result_summary, status)
            VALUES (%s, %s, %s::jsonb, %s, %s)
            """,
            (email, tool_name, json.dumps(params, default=str), summary[:500], status),
        )
    except Exception:
        logger.exception("Failed to log agent activity for tool=%s", tool_name)


def _upsert_job_posting(job: dict) -> None:
    """Upsert a normalized job into job_postings + embed its description."""
    lakebase.run_write(
        """
        INSERT INTO job_postings
            (id, source, external_id, title, company, location, remote,
             salary_min, salary_max, currency, description, url, category,
             posted_at, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            company = EXCLUDED.company,
            location = EXCLUDED.location,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            description = EXCLUDED.description,
            payload = EXCLUDED.payload,
            synced_at = now()
        """,
        (
            job["id"], job["source"], job["external_id"], job["title"],
            job.get("company"), job.get("location"), bool(job.get("remote")),
            job.get("salary_min"), job.get("salary_max"), job.get("currency"),
            job.get("description"), job.get("url"), job.get("category"),
            job.get("posted_at"), json.dumps(job.get("payload", {}), default=str),
        ),
    )
    _upsert_job_embedding(job["id"], job.get("title", ""), job.get("description", ""))


def _upsert_job_embedding(job_id: str, title: str, description: str) -> None:
    """Compute and store the embedding for a job posting."""
    try:
        text = f"{title}\n\n{description}".strip()
        if not text:
            return
        vec = embeddings.encode(text[:4000])  # cap to avoid huge inputs
        lakebase.run_write(
            """
            INSERT INTO job_embeddings (job_id, embedding, model_name)
            VALUES (%s, %s::vector, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                model_name = EXCLUDED.model_name,
                created_at = now()
            """,
            (job_id, str(vec), embeddings.get_model_name()),
        )
    except Exception:
        logger.exception("Failed to embed job_id=%s", job_id)


def _ensure_user(email: str = 'gbarriosarias@gmail.com') -> None:
    """Insert a stub user row if this email is new."""
    lakebase.run_write(
        "INSERT INTO users (email) VALUES (%s) ON CONFLICT (email) DO NOTHING",
        (email,),
    )


# ===========================================================================
#                             SEARCH / RETRIEVAL
# ===========================================================================


@mcp.tool
def search_jobs_all_sources(
    query: str,
    location: str = "",
    remote_only: bool = False,
    salary_min: float = 0,
    limit_per_source: int = 10,
    email: str = 'gbarriosarias@gmail.com',
) -> dict:
    """
    Search Adzuna + USAJobs + RemoteOK for jobs matching the query, upsert
    all results into Lakebase, and return a merged list ranked by source.

    Args:
        query: Free-text search (e.g. "senior python backend").
        location: Optional city/region filter. Ignored by RemoteOK.
        remote_only: If True, only include remote roles. If False, still
            includes them but doesn't filter.
        salary_min: Minimum annual salary filter (USD). 0 = no filter.
        limit_per_source: Max postings per source (default 10).
        email: The user this search is being run for (for activity log).

    Returns:
        {"status", "query", "total", "by_source", "postings"} where
        "postings" is a list of normalized dicts (id, title, company,
        location, remote, salary_min, salary_max, url, source).
    """
    all_postings: list[dict] = []
    by_source: dict[str, int] = {}
    errors: list[str] = []

    for source_name, fn, kwargs in [
        ("adzuna", adzuna_broker.search_jobs,
         {"query": query, "location": location or None,
          "results_per_page": limit_per_source,
          "salary_min": salary_min if salary_min else None}),
        ("usajobs", usajobs_broker.search_jobs,
         {"query": query, "location": location or None,
          "results_per_page": limit_per_source,
          "salary_min": salary_min if salary_min else None,
          "remote_only": remote_only}),
        ("remoteok", remoteok_broker.search_jobs,
         {"query": query, "limit": limit_per_source,
          "salary_min": salary_min if salary_min else None}),
    ]:
        try:
            postings = fn(**kwargs)
            if remote_only and source_name != "remoteok":
                postings = [p for p in postings if p.get("remote")]
            for p in postings:
                _upsert_job_posting(p)
            all_postings.extend(postings)
            by_source[source_name] = len(postings)
        except Exception as e:
            logger.exception("Search failed for source=%s", source_name)
            errors.append(f"{source_name}: {e}")
            by_source[source_name] = 0

    # Persist the search for analytics
    if email:
        try:
            _ensure_user(email)
            lakebase.run_write(
                """
                INSERT INTO saved_searches (email, query, filters, results_count, sources)
                VALUES (%s, %s, %s::jsonb, %s, %s)
                """,
                (
                    email, query,
                    json.dumps({"location": location, "remote_only": remote_only, "salary_min": salary_min}),
                    len(all_postings), list(by_source.keys()),
                ),
            )
        except Exception:
            logger.exception("Failed to log saved search")

    summary = f"Found {len(all_postings)} jobs for '{query}' ({by_source})"
    _log_activity(email or None, "search_jobs_all_sources",
                  {"query": query, "location": location, "remote_only": remote_only},
                  summary)

    return {
        "status": "success" if not errors else "partial",
        "query": query,
        "total": len(all_postings),
        "by_source": by_source,
        "errors": errors,
        "postings": [_public_posting(p) for p in all_postings],
    }


@mcp.tool
def search_adzuna(query: str, location: str = "", salary_min: float = 0, limit: int = 10, email: str = 'gbarriosarias@gmail.com') -> dict:
    """
    Search ONLY Adzuna (public jobs board). Use this when the user
    explicitly wants Adzuna results.

    Args:
        query: Free-text search.
        location: City/region filter. Empty = country-wide.
        salary_min: Minimum salary filter (USD).
        limit: Max results (default 10).
        email: End-user email for activity log.

    Returns:
        {"status", "source": "adzuna", "total", "postings"}.
    """
    try:
        postings = adzuna_broker.search_jobs(
            query=query, location=location or None,
            results_per_page=limit, salary_min=salary_min if salary_min else None,
        )
        for p in postings:
            _upsert_job_posting(p)
        _log_activity(email or None, "search_adzuna", {"query": query},
                      f"Adzuna returned {len(postings)} jobs for '{query}'")
        return {"status": "success", "source": "adzuna", "total": len(postings),
                "postings": [_public_posting(p) for p in postings]}
    except Exception as e:
        _log_activity(email or None, "search_adzuna", {"query": query}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def search_usajobs(query: str, location: str = "", salary_min: float = 0, remote_only: bool = False, limit: int = 10, email: str = 'gbarriosarias@gmail.com') -> dict:
    """
    Search ONLY USAJobs (US federal government openings). Use this when
    the user is looking for public-sector jobs.

    Args:
        query: Free-text keyword.
        location: City/state filter.
        salary_min: Minimum salary (USD).
        remote_only: If True, restrict to telework-eligible positions.
        limit: Max results (default 10).
        email: End-user email for activity log.

    Returns:
        {"status", "source": "usajobs", "total", "postings"}.
    """
    try:
        postings = usajobs_broker.search_jobs(
            query=query, location=location or None,
            results_per_page=limit,
            salary_min=salary_min if salary_min else None,
            remote_only=remote_only,
        )
        for p in postings:
            _upsert_job_posting(p)
        _log_activity(email or None, "search_usajobs", {"query": query},
                      f"USAJobs returned {len(postings)} jobs for '{query}'")
        return {"status": "success", "source": "usajobs", "total": len(postings),
                "postings": [_public_posting(p) for p in postings]}
    except Exception as e:
        _log_activity(email or None, "search_usajobs", {"query": query}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def search_remoteok(query: str, salary_min: float = 0, limit: int = 10, email: str = 'gbarriosarias@gmail.com') -> dict:
    """
    Search ONLY RemoteOK (remote-only tech jobs). Use this when the user
    wants remote roles specifically.

    Args:
        query: Free-text keyword.
        salary_min: Minimum salary (USD).
        limit: Max results (default 10).
        email: End-user email for activity log.

    Returns:
        {"status", "source": "remoteok", "total", "postings"}.
    """
    try:
        postings = remoteok_broker.search_jobs(
            query=query, limit=limit,
            salary_min=salary_min if salary_min else None,
        )
        for p in postings:
            _upsert_job_posting(p)
        _log_activity(email or None, "search_remoteok", {"query": query},
                      f"RemoteOK returned {len(postings)} jobs for '{query}'")
        return {"status": "success", "source": "remoteok", "total": len(postings),
                "postings": [_public_posting(p) for p in postings]}
    except Exception as e:
        _log_activity(email or None, "search_remoteok", {"query": query}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def semantic_search_jobs(query: str, limit: int = 10, email: str = 'gbarriosarias@gmail.com') -> dict:
    """
    Semantic (vector) search over ALL job postings previously synced into
    Lakebase. Use this to find matches using natural language like
    "backend roles that don't require Kubernetes experience".

    Args:
        query: Natural language description of the ideal job.
        limit: Max results to return (default 10).
        email: End-user email for activity log.

    Returns:
        {"status", "query", "total", "postings"} where each posting includes
        a similarity score (0-1, higher = more similar).
    """
    try:
        query_vec = embeddings.encode(query)
        rows = lakebase.run_query(
            """
            SELECT
                p.id, p.source, p.title, p.company, p.location, p.remote,
                p.salary_min, p.salary_max, p.currency, p.url, p.category,
                p.posted_at,
                1 - (e.embedding <=> %s::vector) AS similarity
            FROM job_embeddings e
            JOIN job_postings p ON p.id = e.job_id
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
            """,
            (str(query_vec), str(query_vec), limit),
        )
        _log_activity(email or None, "semantic_search_jobs", {"query": query},
                      f"Semantic search returned {len(rows)} jobs")
        return {"status": "success", "query": query, "total": len(rows), "postings": rows}
    except Exception as e:
        logger.exception("semantic_search_jobs failed")
        _log_activity(email or None, "semantic_search_jobs", {"query": query}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def get_job_details(job_id: str) -> dict:
    """
    Get full details for a single job posting from Lakebase, including the
    full description text and raw API payload.

    Args:
        job_id: The job_postings.id primary key (e.g. "adzuna_...").

    Returns:
        {"status", "job"} with the complete posting or an error message.
    """
    rows = lakebase.run_query(
        "SELECT * FROM job_postings WHERE id = %s LIMIT 1",
        (job_id,),
    )
    if not rows:
        return {"status": "error", "message": f"Job {job_id} not found"}
    return {"status": "success", "job": rows[0]}


# ===========================================================================
#                             WRITES / ACTIONS
# ===========================================================================


@mcp.tool
def upsert_user_profile(
    email: str = 'gbarriosarias@gmail.com',
    name: str = "",
    target_role: str = "",
    remote_ok: bool = True,
    salary_min: float = 0,
    resume_text: str = "",
    skills: list[str] | None = None,
    years_experience: int = 0,
    seniority: str = "",
) -> dict:
    """
    Create or update the current user's profile. This drives future
    job-match scoring and personalized recommendations.

    Args:
        email: User's email address (primary key).
        name: Display name.
        target_role: Role the user is targeting (e.g. "Senior Backend Engineer").
        remote_ok: Whether the user is open to remote roles.
        salary_min: Minimum acceptable annual salary (USD).
        resume_text: Full resume text (used for semantic matching).
        skills: List of skills (e.g. ["python", "postgres", "aws"]).
        years_experience: Total years of experience.
        seniority: One of "junior", "mid", "senior", "staff", "principal".

    Returns:
        {"status", "email", "message"}.
    """
    try:
        _ensure_user(email)
        lakebase.run_write(
            """
            UPDATE users SET
                name = COALESCE(NULLIF(%s, ''), name),
                target_role = COALESCE(NULLIF(%s, ''), target_role),
                remote_ok = %s,
                salary_min = CASE WHEN %s > 0 THEN %s ELSE salary_min END,
                updated_at = now()
            WHERE email = %s
            """,
            (name, target_role, remote_ok, salary_min, salary_min, email),
        )
        lakebase.run_write(
            """
            INSERT INTO profiles (email, resume_text, skills, years_experience, seniority)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET
                resume_text = COALESCE(NULLIF(EXCLUDED.resume_text, ''), profiles.resume_text),
                skills = COALESCE(EXCLUDED.skills, profiles.skills),
                years_experience = CASE WHEN EXCLUDED.years_experience > 0
                    THEN EXCLUDED.years_experience ELSE profiles.years_experience END,
                seniority = COALESCE(NULLIF(EXCLUDED.seniority, ''), profiles.seniority),
                updated_at = now()
            """,
            (email, resume_text, skills or None, years_experience or 0, seniority),
        )
        _log_activity(email, "upsert_user_profile", {"target_role": target_role},
                      f"Profile updated for {email}")
        return {"status": "success", "email": email, "message": "Profile saved"}
    except Exception as e:
        logger.exception("upsert_user_profile failed")
        _log_activity(email, "upsert_user_profile", {}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def save_job_to_pipeline(email: str = 'gbarriosarias@gmail.com', job_id: str, stage: str = "saved", match_reasoning: str = "") -> dict:
    """
    Save a job to the user's application pipeline in the specified stage.

    Stages: 'saved', 'applied', 'interviewing', 'offer', 'rejected'.
    If the (email, job_id) pair already exists, its stage is UPDATED.

    Args:
        email: User's email.
        job_id: job_postings.id to save.
        stage: Pipeline stage (default "saved").
        match_reasoning: Optional short explanation of why this job is a match.

    Returns:
        {"status", "application_id", "stage", "message"}.
    """
    valid_stages = {"saved", "applied", "interviewing", "offer", "rejected"}
    if stage not in valid_stages:
        return {"status": "error", "message": f"stage must be one of {sorted(valid_stages)}"}
    try:
        _ensure_user(email)
        row = lakebase.run_write_returning(
            """
            INSERT INTO applications (email, job_id, stage, match_reasoning)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email, job_id) DO UPDATE SET
                stage = EXCLUDED.stage,
                match_reasoning = COALESCE(NULLIF(EXCLUDED.match_reasoning, ''), applications.match_reasoning),
                updated_at = now()
            RETURNING id, stage
            """,
            (email, job_id, stage, match_reasoning),
        )
        _log_activity(email, "save_job_to_pipeline",
                      {"job_id": job_id, "stage": stage},
                      f"Saved job {job_id} in stage '{stage}'")
        return {"status": "success", "application_id": row["id"], "stage": row["stage"],
                "message": f"Job saved in stage '{stage}'"}
    except Exception as e:
        logger.exception("save_job_to_pipeline failed")
        _log_activity(email, "save_job_to_pipeline", {"job_id": job_id}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def update_application_stage(email: str = 'gbarriosarias@gmail.com', job_id: str, new_stage: str) -> dict:
    """
    Move an existing application to a new pipeline stage.

    Args:
        email: User's email.
        job_id: job_postings.id.
        new_stage: One of 'saved', 'applied', 'interviewing', 'offer', 'rejected'.

    Returns:
        {"status", "message"}.
    """
    valid = {"saved", "applied", "interviewing", "offer", "rejected"}
    if new_stage not in valid:
        return {"status": "error", "message": f"new_stage must be one of {sorted(valid)}"}
    try:
        affected = lakebase.run_write(
            "UPDATE applications SET stage = %s, updated_at = now() WHERE email = %s AND job_id = %s",
            (new_stage, email, job_id),
        )
        if affected == 0:
            return {"status": "not_found", "message": f"No application for {email}/{job_id}"}
        _log_activity(email, "update_application_stage",
                      {"job_id": job_id, "new_stage": new_stage},
                      f"Advanced job {job_id} to '{new_stage}'")
        return {"status": "success", "message": f"Application moved to '{new_stage}'"}
    except Exception as e:
        _log_activity(email, "update_application_stage", {"job_id": job_id}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def add_interview_note(email: str = 'gbarriosarias@gmail.com', job_id: str, note: str, interview_date: str = "") -> dict:
    """
    Attach a free-text interview note to an existing application.

    Args:
        email: User's email.
        job_id: job_postings.id (must already exist in applications).
        note: Free-text note (interview feedback, follow-ups, etc.).
        interview_date: Optional YYYY-MM-DD date for the interview.

    Returns:
        {"status", "note_id", "message"}.
    """
    try:
        app_rows = lakebase.run_query(
            "SELECT id FROM applications WHERE email = %s AND job_id = %s",
            (email, job_id),
        )
        if not app_rows:
            return {"status": "not_found",
                    "message": f"No application for {email}/{job_id}. Save the job first."}
        app_id = app_rows[0]["id"]
        row = lakebase.run_write_returning(
            """
            INSERT INTO interview_notes (application_id, email, note, interview_date)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (app_id, email, note, interview_date or None),
        )
        _log_activity(email, "add_interview_note", {"job_id": job_id},
                      f"Added interview note for {job_id}")
        return {"status": "success", "note_id": row["id"], "message": "Interview note saved"}
    except Exception as e:
        _log_activity(email, "add_interview_note", {"job_id": job_id}, str(e), status="error")
        return {"status": "error", "message": str(e)}


@mcp.tool
def draft_cover_letter(email: str = 'gbarriosarias@gmail.com', job_id: str) -> dict:
    """
    Draft a tailored cover letter for a specific job, using the user's
    profile (resume_text, skills, seniority, target_role) as context.
    The draft is stored in Lakebase for retrieval by the dashboard.

    The letter follows a template - substantive personalization is
    expected to come from the calling LLM agent, which may re-invoke this
    tool with an improved draft after review.

    Args:
        email: User's email.
        job_id: job_postings.id of the target role.

    Returns:
        {"status", "cover_letter_id", "cover_letter_text"}.
    """
    try:
        job_rows = lakebase.run_query(
            "SELECT title, company, description FROM job_postings WHERE id = %s",
            (job_id,),
        )
        if not job_rows:
            return {"status": "error", "message": f"Job {job_id} not found"}
        job = job_rows[0]

        profile_rows = lakebase.run_query(
            """
            SELECT u.name, u.target_role, p.skills, p.seniority, p.years_experience, p.resume_text
            FROM users u LEFT JOIN profiles p ON u.email = p.email
            WHERE u.email = %s
            """,
            (email,),
        )
        profile = profile_rows[0] if profile_rows else {}
        name = profile.get("name") or "the candidate"
        skills = profile.get("skills") or []
        skills_str = ", ".join(skills[:8]) if skills else "relevant technical skills"
        seniority = profile.get("seniority") or "professional"
        years = profile.get("years_experience") or 0

        cover_letter = (
            f"Dear {job['company'] or 'Hiring Manager'} Team,\n\n"
            f"I'm writing to express my strong interest in the {job['title']} role. "
            f"As a {seniority} with {years or 'several'} years of experience specializing in "
            f"{skills_str}, I believe my background aligns well with what you're looking for.\n\n"
            f"In my recent work I have focused on delivering measurable outcomes through "
            f"clean engineering, cross-team collaboration, and a bias toward action. "
            f"I'm excited by the opportunity to bring that same energy to {job['company'] or 'your team'}, "
            f"particularly given the scope of the role you've described.\n\n"
            f"I would welcome the chance to discuss how my experience with {skills_str} "
            f"can help drive results. Thank you for your time and consideration.\n\n"
            f"Sincerely,\n{name}"
        )

        row = lakebase.run_write_returning(
            """
            INSERT INTO cover_letters (email, job_id, cover_letter_text)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (email, job_id, cover_letter),
        )
        _log_activity(email, "draft_cover_letter", {"job_id": job_id},
                      f"Drafted cover letter for {job['title']}")
        return {"status": "success", "cover_letter_id": row["id"], "cover_letter_text": cover_letter}
    except Exception as e:
        logger.exception("draft_cover_letter failed")
        _log_activity(email, "draft_cover_letter", {"job_id": job_id}, str(e), status="error")
        return {"status": "error", "message": str(e)}


# ===========================================================================
#                         ANALYTICS / REASONING
# ===========================================================================


@mcp.tool
def explain_job_match(email: str = 'gbarriosarias@gmail.com', job_id: str) -> dict:
    """
    Score how well a job matches the user's profile using cosine similarity
    between the job embedding and a synthesized "user profile embedding"
    (skills + target_role + resume text). Also returns skill overlap.

    This gives the agent a quantitative signal it can weave into a natural
    language explanation for the user.

    Args:
        email: User's email.
        job_id: job_postings.id to score.

    Returns:
        {"status", "match_score" (0-1), "skills_overlap", "job_summary"}.
    """
    try:
        profile_rows = lakebase.run_query(
            """
            SELECT u.target_role, p.skills, p.resume_text
            FROM users u LEFT JOIN profiles p ON u.email = p.email
            WHERE u.email = %s
            """,
            (email,),
        )
        if not profile_rows:
            return {"status": "error", "message": f"No profile for {email}"}
        profile = profile_rows[0]
        skills = profile.get("skills") or []
        profile_text = " ".join([
            profile.get("target_role") or "",
            " ".join(skills),
            (profile.get("resume_text") or "")[:2000],
        ]).strip()
        if not profile_text:
            return {"status": "error", "message": "Profile has no text to match against."}

        profile_vec = embeddings.encode(profile_text)
        rows = lakebase.run_query(
            """
            SELECT p.title, p.company, p.description,
                   1 - (e.embedding <=> %s::vector) AS similarity
            FROM job_embeddings e JOIN job_postings p ON p.id = e.job_id
            WHERE p.id = %s
            """,
            (str(profile_vec), job_id),
        )
        if not rows:
            return {"status": "error", "message": f"No embedding for {job_id}"}
        row = rows[0]

        job_desc_lower = (row.get("description") or "").lower()
        overlap = [s for s in skills if s.lower() in job_desc_lower]

        return {
            "status": "success",
            "match_score": round(float(row["similarity"]), 4),
            "skills_overlap": overlap,
            "job_summary": {"title": row["title"], "company": row["company"]},
        }
    except Exception as e:
        logger.exception("explain_job_match failed")
        return {"status": "error", "message": str(e)}


@mcp.tool
def find_stale_applications(email: str = 'gbarriosarias@gmail.com', days_stale: int = 7) -> dict:
    """
    Find applications that haven't been updated in `days_stale` days and
    are not in a terminal stage (offer/rejected). Useful to nudge users
    to follow up.

    Args:
        email: User's email.
        days_stale: Threshold in days (default 7).

    Returns:
        {"status", "days_stale", "total_stale", "applications"}.
    """
    try:
        rows = lakebase.run_query(
            """
            SELECT a.id, a.stage, a.updated_at,
                   p.title, p.company, p.url,
                   EXTRACT(EPOCH FROM (now() - a.updated_at))/86400 AS days_since_update
            FROM applications a JOIN job_postings p ON p.id = a.job_id
            WHERE a.email = %s
              AND a.stage NOT IN ('offer', 'rejected')
              AND a.updated_at < now() - (%s || ' days')::interval
            ORDER BY a.updated_at ASC
            """,
            (email, str(int(days_stale))),
        )
        return {"status": "success", "days_stale": days_stale,
                "total_stale": len(rows), "applications": rows}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool
def get_pipeline_summary(email: str = 'gbarriosarias@gmail.com') -> dict:
    """
    Get a quick summary of the user's application pipeline: counts by stage,
    total applications, total saved jobs, and stale count (>7 days no update).

    Args:
        email: User's email.

    Returns:
        {"status", "by_stage", "totals"}.
    """
    try:
        stage_rows = lakebase.run_query(
            "SELECT stage, COUNT(*) AS count FROM applications WHERE email = %s GROUP BY stage",
            (email,),
        )
        by_stage = {r["stage"]: int(r["count"]) for r in stage_rows}
        stale = lakebase.run_query(
            """
            SELECT COUNT(*) AS c FROM applications
            WHERE email = %s AND stage NOT IN ('offer','rejected')
              AND updated_at < now() - INTERVAL '7 days'
            """,
            (email,),
        )
        return {
            "status": "success",
            "by_stage": by_stage,
            "totals": {
                "total_applications": sum(by_stage.values()),
                "in_flight": by_stage.get("applied", 0) + by_stage.get("interviewing", 0),
                "stale_over_7d": int(stale[0]["c"]) if stale else 0,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# Response shaping helpers
# ---------------------------------------------------------------------------


def _public_posting(p: dict) -> dict:
    """Trim a normalized posting dict down to the fields the agent needs."""
    return {
        "id": p["id"],
        "source": p["source"],
        "title": p["title"],
        "company": p.get("company"),
        "location": p.get("location"),
        "remote": p.get("remote"),
        "salary_min": p.get("salary_min"),
        "salary_max": p.get("salary_max"),
        "currency": p.get("currency"),
        "url": p.get("url"),
        "posted_at": p.get("posted_at"),
        "description_snippet": (p.get("description") or "")[:500],
    }


# ---------------------------------------------------------------------------
# Server startup (same pattern as Day 3 alpaca_mcp_server.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if hasattr(mcp, "app") and mcp.app is not None:
        mcp.app.add_middleware(RequestContextMiddleware)

    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    mcp.run(transport="http", host="0.0.0.0", port=port)
