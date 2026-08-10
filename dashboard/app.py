"""
AI Job Hunting Copilot - Dashboard.

Read-only Flask app that reflects, in near real time, everything the
Agent Bricks agent is doing through the MCP server. It never calls the
job APIs directly - it only queries Lakebase, which is the shared source
of truth between the MCP server (writes) and this dashboard (reads).

Deploy as its OWN Databricks App (separate from mcp_server/).

Run locally:
    python app.py
"""

import json
import logging
import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job-dashboard")

app = Flask(__name__)
_w = WorkspaceClient()

DEFAULT_EMAIL = os.environ.get("DEFAULT_USER_EMAIL", "gbarriosarias@gmail.com")


def _current_user_email() -> str:
    """Resolve the current user - prefer Databricks App identity headers."""
    email = request.headers.get("X-Forwarded-Email")
    if email:
        return email
    email = request.args.get("email")
    if email:
        return email
    try:
        return _w.current_user.me().user_name or DEFAULT_EMAIL
    except Exception:
        return DEFAULT_EMAIL


@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception")
    status = getattr(err, "code", 500)
    if not isinstance(status, int):
        status = 500
    return jsonify({"error": str(err)}), status


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.route("/")
def index():
    return render_template("index.html", default_email=_current_user_email())


# ---------------------------------------------------------------------------
# API endpoints - all read-only, all filter by ?email=
# ---------------------------------------------------------------------------


@app.route("/api/summary")
def api_summary():
    """High-level KPIs for the top of the dashboard."""
    email = request.args.get("email") or _current_user_email()

    by_stage_rows = lakebase.run_query(
        "SELECT stage, COUNT(*) AS c FROM applications WHERE email = %s GROUP BY stage",
        (email,),
    )
    by_stage = {r["stage"]: int(r["c"]) for r in by_stage_rows}

    totals = lakebase.run_query(
        """
        SELECT
            (SELECT COUNT(*) FROM applications WHERE email = %s) AS total_apps,
            (SELECT COUNT(*) FROM cover_letters WHERE email = %s) AS total_cover_letters,
            (SELECT COUNT(*) FROM saved_searches WHERE email = %s) AS total_searches,
            (SELECT COUNT(*) FROM applications WHERE email = %s
                AND stage NOT IN ('offer','rejected')
                AND updated_at < now() - INTERVAL '7 days') AS stale_over_7d
        """,
        (email, email, email, email),
    )
    totals_row = totals[0] if totals else {}

    return jsonify({
        "email": email,
        "by_stage": by_stage,
        "totals": {k: int(v) if v is not None else 0 for k, v in dict(totals_row).items()},
    })


@app.route("/api/pipeline")
def api_pipeline():
    """Full pipeline: every application with the job info the UI needs."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        """
        SELECT a.id, a.stage, a.match_score, a.match_reasoning,
               a.created_at, a.updated_at,
               p.id AS job_id, p.title, p.company, p.location, p.remote,
               p.salary_min, p.salary_max, p.currency, p.url, p.source
        FROM applications a JOIN job_postings p ON p.id = a.job_id
        WHERE a.email = %s
        ORDER BY a.updated_at DESC
        """,
        (email,),
    )
    return jsonify({"email": email, "count": len(rows), "applications": rows})


@app.route("/api/activity")
def api_activity():
    """Live feed of agent tool calls for this user."""
    email = request.args.get("email") or _current_user_email()
    limit = int(request.args.get("limit", 50))
    rows = lakebase.run_query(
        """
        SELECT id, tool_name, params, result_summary, status, created_at
        FROM agent_activity_log
        WHERE email = %s OR email IS NULL
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (email, limit),
    )
    return jsonify({"email": email, "activity": rows})


@app.route("/api/source_breakdown")
def api_source_breakdown():
    """Counts of job postings that have made it into the pipeline, by source."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        """
        SELECT p.source, COUNT(*) AS c
        FROM applications a JOIN job_postings p ON p.id = a.job_id
        WHERE a.email = %s
        GROUP BY p.source
        """,
        (email,),
    )
    return jsonify({"by_source": {r["source"]: int(r["c"]) for r in rows}})


@app.route("/api/stale")
def api_stale():
    """Applications not touched in >7 days that aren't in a terminal stage."""
    email = request.args.get("email") or _current_user_email()
    days = int(request.args.get("days", 7))
    rows = lakebase.run_query(
        """
        SELECT a.id, a.stage, a.updated_at, p.title, p.company, p.url,
               EXTRACT(EPOCH FROM (now() - a.updated_at))/86400 AS days_since_update
        FROM applications a JOIN job_postings p ON p.id = a.job_id
        WHERE a.email = %s
          AND a.stage NOT IN ('offer','rejected')
          AND a.updated_at < now() - (%s || ' days')::interval
        ORDER BY a.updated_at ASC
        LIMIT 25
        """,
        (email, str(days)),
    )
    return jsonify({"stale": rows, "days_stale": days})


@app.route("/api/cover_letters")
def api_cover_letters():
    """Recently generated cover letters for this user."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        """
        SELECT cl.id, cl.job_id, cl.cover_letter_text, cl.created_at,
               p.title, p.company
        FROM cover_letters cl LEFT JOIN job_postings p ON p.id = cl.job_id
        WHERE cl.email = %s
        ORDER BY cl.created_at DESC
        LIMIT 20
        """,
        (email,),
    )
    return jsonify({"cover_letters": rows})


@app.route("/api/searches")
def api_searches():
    """Recent search queries this user has run through the agent."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        """
        SELECT id, query, filters, results_count, sources, created_at
        FROM saved_searches
        WHERE email = %s
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (email,),
    )
    return jsonify({"searches": rows})


@app.route("/api/top_matches")
def api_top_matches():
    """
    Top job postings not yet in the user's pipeline, ranked by cosine
    similarity between the user's synthesized profile embedding and each
    job embedding. This is what "great job matches for you right now" means.

    Falls back to most-recent postings if the user has no embeddable profile.
    """
    email = request.args.get("email") or _current_user_email()
    limit = int(request.args.get("limit", 10))

    # Try semantic ranking first
    profile = lakebase.run_query(
        """
        SELECT u.target_role, p.skills, p.resume_text
        FROM users u LEFT JOIN profiles p ON u.email = p.email
        WHERE u.email = %s
        """,
        (email,),
    )
    if profile:
        row = profile[0]
        skills = row.get("skills") or []
        text = " ".join([
            row.get("target_role") or "",
            " ".join(skills),
            (row.get("resume_text") or "")[:2000],
        ]).strip()
        if text:
            try:
                # Lazy-load embeddings only if we need them
                from sentence_transformers import SentenceTransformer
                model_name = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
                model = SentenceTransformer(model_name)
                vec = model.encode(text).tolist()
                rows = lakebase.run_query(
                    """
                    SELECT p.id, p.title, p.company, p.location, p.remote,
                           p.salary_min, p.salary_max, p.url, p.source,
                           1 - (e.embedding <=> %s::vector) AS similarity
                    FROM job_embeddings e JOIN job_postings p ON p.id = e.job_id
                    WHERE p.id NOT IN (
                        SELECT job_id FROM applications WHERE email = %s
                    )
                    ORDER BY e.embedding <=> %s::vector
                    LIMIT %s
                    """,
                    (str(vec), email, str(vec), limit),
                )
                return jsonify({"matches": rows, "ranking": "semantic"})
            except Exception:
                logger.exception("Semantic ranking failed - falling back")

    # Fallback: most recent postings not yet applied to
    rows = lakebase.run_query(
        """
        SELECT p.id, p.title, p.company, p.location, p.remote,
               p.salary_min, p.salary_max, p.url, p.source, p.posted_at
        FROM job_postings p
        WHERE p.id NOT IN (SELECT job_id FROM applications WHERE email = %s)
        ORDER BY p.synced_at DESC
        LIMIT %s
        """,
        (email, limit),
    )
    return jsonify({"matches": rows, "ranking": "recent"})


# ---------------------------------------------------------------------------
# Interactive endpoints - WRITES from the dashboard UI
# ---------------------------------------------------------------------------

VALID_STAGES = {"saved", "applied", "interviewing", "offer", "rejected"}


def _log_dashboard_action(email: str, tool_name: str, params: dict, summary: str) -> None:
    """Log dashboard-driven actions to the same agent_activity_log so
    the activity feed shows human + agent actions side-by-side."""
    try:
        lakebase.run_write(
            """
            INSERT INTO agent_activity_log (email, tool_name, params, result_summary, status)
            VALUES (%s, %s, %s::jsonb, %s, 'success')
            """,
            (email, f"[dashboard] {tool_name}", json.dumps(params, default=str), summary[:500]),
        )
    except Exception:
        logging.exception("Failed to log dashboard action")


@app.route("/api/applications/<int:app_id>/stage", methods=["POST"])
def api_update_stage(app_id: int):
    """Move an application to a new stage (used by kanban drag+drop and dropdown)."""
    body = request.json or {}
    new_stage = (body.get("stage") or "").strip().lower()
    if new_stage not in VALID_STAGES:
        return jsonify({"error": f"stage must be one of {sorted(VALID_STAGES)}"}), 400

    row = lakebase.run_query(
        "SELECT a.email, p.title, p.company FROM applications a "
        "JOIN job_postings p ON p.id = a.job_id WHERE a.id = %s",
        (app_id,),
    )
    if not row:
        return jsonify({"error": "application not found"}), 404
    meta = row[0]

    affected = lakebase.run_write(
        "UPDATE applications SET stage = %s, updated_at = now() WHERE id = %s",
        (new_stage, app_id),
    )
    if affected == 0:
        return jsonify({"error": "application not found"}), 404

    _log_dashboard_action(
        meta["email"], "update_stage",
        {"application_id": app_id, "new_stage": new_stage},
        f"Moved '{meta.get('title')}' at {meta.get('company')} to '{new_stage}'",
    )
    return jsonify({"status": "success", "stage": new_stage})


@app.route("/api/applications/<int:app_id>", methods=["DELETE"])
def api_delete_application(app_id: int):
    """Remove an application from the pipeline."""
    row = lakebase.run_query(
        "SELECT a.email, p.title, p.company FROM applications a "
        "JOIN job_postings p ON p.id = a.job_id WHERE a.id = %s",
        (app_id,),
    )
    if not row:
        return jsonify({"error": "application not found"}), 404
    meta = row[0]

    affected = lakebase.run_write(
        "DELETE FROM applications WHERE id = %s",
        (app_id,),
    )
    if affected == 0:
        return jsonify({"error": "application not found"}), 404

    _log_dashboard_action(
        meta["email"], "delete_application",
        {"application_id": app_id},
        f"Removed '{meta.get('title')}' at {meta.get('company')} from pipeline",
    )
    return jsonify({"status": "success"})


@app.route("/api/applications/<int:app_id>/note", methods=["POST"])
def api_add_note(app_id: int):
    """Attach a note to an application from the dashboard."""
    body = request.json or {}
    note = (body.get("note") or "").strip()
    if not note:
        return jsonify({"error": "note is required"}), 400

    row = lakebase.run_query(
        "SELECT email FROM applications WHERE id = %s",
        (app_id,),
    )
    if not row:
        return jsonify({"error": "application not found"}), 404
    email = row[0]["email"]

    lakebase.run_write(
        """
        INSERT INTO interview_notes (application_id, email, note)
        VALUES (%s, %s, %s)
        """,
        (app_id, email, note),
    )
    _log_dashboard_action(
        email, "add_note",
        {"application_id": app_id},
        f"Added note to application #{app_id}",
    )
    return jsonify({"status": "success"})


@app.route("/api/velocity")
def api_velocity():
    """Weekly velocity: new applications per week over the last 8 weeks."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        """
        WITH weeks AS (
            SELECT generate_series(
                date_trunc('week', now() - INTERVAL '7 weeks'),
                date_trunc('week', now()),
                INTERVAL '1 week'
            ) AS week_start
        )
        SELECT
            to_char(w.week_start, 'MM/DD') AS label,
            w.week_start,
            COUNT(a.id) AS new_apps
        FROM weeks w
        LEFT JOIN applications a
            ON a.email = %s
           AND date_trunc('week', a.created_at) = w.week_start
        GROUP BY w.week_start
        ORDER BY w.week_start
        """,
        (email,),
    )
    return jsonify({
        "labels": [r["label"] for r in rows],
        "values": [int(r["new_apps"]) for r in rows],
    })


@app.route("/api/funnel")
def api_funnel():
    """Cumulative conversion funnel: saved -> applied -> interviewing -> offer.
    Interpretation: each stage's count includes applications that later moved
    forward, so we approximate cumulative touch counts."""
    email = request.args.get("email") or _current_user_email()
    rows = lakebase.run_query(
        "SELECT stage, COUNT(*) AS c FROM applications WHERE email = %s GROUP BY stage",
        (email,),
    )
    counts = {r["stage"]: int(r["c"]) for r in rows}
    # Cumulative funnel semantics: everything in interviewing was once "applied",
    # everything in offer was once "interviewing", etc.
    saved         = sum(counts.values()) - counts.get("rejected", 0)
    applied       = counts.get("applied", 0) + counts.get("interviewing", 0) + counts.get("offer", 0)
    interviewing  = counts.get("interviewing", 0) + counts.get("offer", 0)
    offer         = counts.get("offer", 0)

    def rate(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0

    return jsonify({
        "stages": [
            {"label": "Saved", "value": saved},
            {"label": "Applied", "value": applied},
            {"label": "Interviewing", "value": interviewing},
            {"label": "Offer", "value": offer},
        ],
        "rates": {
            "saved_to_applied": rate(applied, saved),
            "applied_to_interviewing": rate(interviewing, applied),
            "interviewing_to_offer": rate(offer, interviewing),
        },
    })


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8001)))
    app.run(debug=False, host="0.0.0.0", port=port)
