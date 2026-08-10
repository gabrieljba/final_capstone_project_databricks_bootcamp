# AI Job Hunting Copilot — Databricks Capstone

An agentic job-search assistant built on **Databricks Apps + Lakebase + Agent Bricks**.

- The **agent** searches jobs across **Adzuna**, **USAJobs**, and **RemoteOK**, embeds
  descriptions with sentence-transformers, saves matches to a per-user pipeline in
  Lakebase, drafts tailored cover letters, and tracks interview notes.
- The **interactive dashboard** (a separate Databricks App) reads AND writes the
  same Lakebase database. It reflects the agent's activity live and lets a human
  user collaborate: a drag-and-drop **Kanban board**, filters, quick-edit dropdowns,
  delete + note buttons per card, a conversion **funnel** with rates, a weekly
  **velocity** chart, source breakdown, stale-application detection, semantic-
  ranked top matches, unified agent+human activity feed, and generated cover letters.

## Architecture

```
Agent Bricks agent  --(MCP tool calls)-->  mcp_server/job_mcp_server.py
                                                    |
                                                    |----> Adzuna API
                                                    |----> USAJobs API
                                                    |----> RemoteOK API
                                                    |
                                                    +----> Lakebase (writes)
                                                                ^
                                                                | reads + writes
Dashboard user   -->   dashboard/app.py   ---------+
```

Both apps deploy independently as **Databricks Apps**, share the same Lakebase database,
and use identical secret-scope patterns.

## Repo Layout

```
final_capstone_project_databricks_bootcamp/
├── README.md
├── schema.sql                 # Create tables (run in Lakebase)
├── setup_secrets.py           # Store API keys as Databricks secrets
├── mcp_server/
│   ├── job_mcp_server.py      # FastMCP server, 14 @mcp.tool decorators
│   ├── adzuna_broker.py       # Adzuna API adapter
│   ├── usajobs_broker.py      # USAJobs API adapter
│   ├── remoteok_broker.py     # RemoteOK API adapter
│   ├── embeddings.py          # Lazy-loaded sentence-transformers wrapper
│   ├── lakebase.py            # Postgres connection helper
│   ├── app.yaml               # Databricks App config
│   └── requirements.txt
└── dashboard/
    ├── app.py                 # Flask dashboard (read + interactive write endpoints)
    ├── lakebase.py
    ├── templates/index.html   # Kanban + charts + filters + modals (Chart.js, live-refresh)
    ├── app.yaml
    └── requirements.txt
```

## Dashboard Features

**Read-only views (auto-refresh every 15s)**

- 5 KPI cards: total applications, in-flight, stale >7d, cover letters, searches
- **Conversion funnel** with per-stage counts and Saved→Applied, Applied→Interview, Interview→Offer rates
- **Weekly velocity** line chart (last 8 weeks of new applications)
- **Jobs by source** doughnut chart (Adzuna vs USAJobs vs RemoteOK)
- Stale applications, top semantic matches, recent searches, generated cover letters
- Unified activity feed showing **agent tool calls** and **human dashboard actions** side-by-side

**Interactive workspace**

- **Kanban board** with 5 columns (Saved / Applied / Interviewing / Offer / Rejected)
- Drag & drop cards between columns to change stage — persists instantly
- **Filters**: search text (title/company), source, remote/on-site
- Per-card actions: open posting (↗), add note (📝), delete (✕)
- **Quick-edit table** underneath the Kanban with stage dropdowns per row
- Confirmation modals for destructive actions
- Toast notifications for every write

**Interactive dashboard endpoints (writes)**

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST   | `/api/applications/<id>/stage` | Change pipeline stage (Kanban drop / dropdown) |
| DELETE | `/api/applications/<id>` | Remove from pipeline |
| POST   | `/api/applications/<id>/note` | Attach an interview note |

Every human write is logged into `agent_activity_log` with a `[dashboard]` prefix
so the activity feed shows human + agent contributions in one timeline.

## MCP Tools (14 total)

**Search / retrieval**

| Tool | Purpose |
|------|---------|
| `search_jobs_all_sources` | Query Adzuna + USAJobs + RemoteOK in parallel, upsert everything, return merged results |
| `search_adzuna` | Search only Adzuna |
| `search_usajobs` | Search only USAJobs (federal roles) |
| `search_remoteok` | Search only RemoteOK (remote tech roles) |
| `semantic_search_jobs` | pgvector cosine similarity over synced job descriptions |
| `get_job_details` | Full posting including raw payload |

**Writes / actions**

| Tool | Purpose |
|------|---------|
| `upsert_user_profile` | Create/update user + skills + resume + target role |
| `save_job_to_pipeline` | Save a job with a stage (saved/applied/interviewing/offer/rejected) |
| `update_application_stage` | Move an existing application to a new stage |
| `add_interview_note` | Attach free-text notes to an application |
| `draft_cover_letter` | Generate + persist a tailored cover letter |

**Analytics / reasoning**

| Tool | Purpose |
|------|---------|
| `explain_job_match` | Cosine similarity score + skills overlap between user profile and a job |
| `find_stale_applications` | Applications not updated in N days and not in a terminal stage |
| `get_pipeline_summary` | Counts by stage + in-flight + stale total |

Every tool call is logged into `agent_activity_log` so the dashboard renders a live feed.

## Suggested Agent System Prompt

```
You are an AI Job Hunting Copilot. You help users find jobs, track applications,
and prepare for interviews using the tools available to you.

WORKFLOW:
1. When a new user talks to you, call get_pipeline_summary(email) to understand
   their current state. If empty, ask if they want to set up their profile via
   upsert_user_profile.
2. For any job search request, prefer search_jobs_all_sources (broadest coverage).
   Use single-source tools only when the user explicitly names one.
3. After a search, offer to save top matches with save_job_to_pipeline (default
   stage: "saved"). Use explain_job_match to justify why each is a good fit.
4. When the user asks about their pipeline, use get_pipeline_summary +
   find_stale_applications to give a concise status.
5. When the user is preparing to apply, offer to call draft_cover_letter.
6. When the user reports interview feedback, call add_interview_note.
7. When stage changes ("I got rejected", "moved to interview"), call
   update_application_stage.

GUARDRAILS:
- NEVER invent job postings, salaries, or company names. Only reference what
  the tools return.
- If a tool returns status="error", tell the user honestly and suggest a next step.
- Always pass the user's email to every tool that accepts it.
- Do not run destructive actions without user confirmation (though currently
  none of the tools delete data).
```

---

# STEP-BY-STEP: What to run in Databricks

Follow these steps in order. Any step marked **YOU** requires you to do
something manually in the Databricks UI or a notebook.

## Step 1 — Create Lakebase tables

**YOU** — Open a SQL editor (Databricks SQL, DBeaver, or a Python notebook with `psycopg2`)
connected to your Lakebase Postgres database. Run the entire `schema.sql` file.
It creates 9 tables + indexes + the pgvector extension.

Verify with:

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'users','profiles','job_postings','job_embeddings',
      'applications','interview_notes','cover_letters',
      'saved_searches','agent_activity_log'
  );
```

You should see 9 rows.

## Step 2 — Store secrets

**YOU** — Open a Databricks notebook. Copy the contents of `setup_secrets.py`
into a cell, fill in your **plaintext** values in the constants at the top, then
run the cell.

Secrets that will be created:

| Scope | Key | Notes |
|-------|-----|-------|
| `database` | `lakebase-url` | Your existing Postgres URL (skip if already set) |
| `jobs` | `adzuna-app-id` | From https://developer.adzuna.com/ |
| `jobs` | `adzuna-app-key` | From https://developer.adzuna.com/ |
| `jobs` | `usajobs-api-key` | From https://developer.usajobs.gov/apirequest/ |
| `jobs` | `usajobs-user-agent` | The email you registered with USAJobs (they use it as User-Agent) |

Verify with:

```bash
databricks secrets list-secrets database
databricks secrets list-secrets jobs
```

## Step 3 — Deploy the MCP server as a Databricks App

**YOU** — In Databricks:

1. Go to **Compute → Apps → Create App**.
2. Name: `ai-job-copilot-mcp` (or anything you prefer).
3. Source: point it at the `mcp_server/` folder from this repo. Upload the
   folder to your workspace first (e.g. `/Workspace/Users/<you>/capstone/mcp_server/`).
4. Databricks will read `app.yaml` and install `requirements.txt` automatically.
5. Once deployed, copy the app URL (looks like `https://ai-job-copilot-mcp-<hash>.databricksapps.com`).

**Expected startup log** should end with something like:
`INFO Uvicorn running on http://0.0.0.0:8000` — this means FastMCP started successfully.

## Step 4 — Deploy the dashboard as a second Databricks App

**YOU** — Same as Step 3, but with the `dashboard/` folder:

1. **Compute → Apps → Create App**.
2. Name: `ai-job-copilot-dashboard`.
3. Source: `dashboard/` folder.
4. Open the dashboard URL in your browser — you should see the empty dashboard
   with your email pre-filled.

## Step 5 — Register the MCP server as an External MCP in Agent Bricks

**YOU** — In Databricks:

1. Go to **Agent Bricks → Tools → Add MCP Server**.
2. URL: the MCP server app URL from Step 3, ending in `/mcp` (append it if needed).
3. Agent Bricks will auto-discover the 14 tools.

## Step 6 — Create the Agent Bricks agent

**YOU** — In Databricks:

1. Go to **Agent Bricks → Create Agent**.
2. Attach the MCP server registered in Step 5 as an external tool.
3. Paste the **system prompt** from the "Suggested Agent System Prompt" section
   above.
4. Save + deploy.

## Step 7 — Try it end-to-end

Chat with the agent:

```
Hi! My email is <your-email>. I'm a senior backend engineer, 8 years experience,
skills: python, postgres, aws, kubernetes. Target role: Staff Backend Engineer.
Salary min $200k, remote only. Set up my profile then find me some jobs.
```

Expected agent behavior:

1. Calls `upsert_user_profile(...)` → dashboard now shows your email.
2. Calls `search_jobs_all_sources(...)` → dashboard "Recent Searches" shows the query,
   "Jobs by Source" chart appears.
3. Calls `explain_job_match(...)` on top candidates → agent shows match scores.
4. Calls `save_job_to_pipeline(...)` for chosen jobs → dashboard "Pipeline"
   populates with saved rows, "Total Applications" KPI increments.
5. Every one of these actions shows up in the **Recent Agent Activity** feed
   with timestamps.

## Troubleshooting

**App fails to start** — Ensure `app.yaml` contains both `command:` AND
`resources: [{name: requirements, source: {path: ./requirements.txt}}]`.
Without the resources block Databricks does NOT install `requirements.txt`.

**"vector extension does not exist"** — Run `CREATE EXTENSION IF NOT EXISTS vector;`
in your Lakebase database (already included at the top of `schema.sql`).

**USAJobs returns 403** — The `usajobs-user-agent` secret must be a valid email
registered at https://developer.usajobs.gov/apirequest/. USAJobs enforces this.

**Adzuna returns 400** — Verify both `adzuna-app-id` and `adzuna-app-key`
are stored as base64 (the `setup_secrets.py` script does this automatically).

**Dashboard says "No agent activity yet"** — The agent hasn't called any tools
for that email yet. Ask the agent to do something. The feed refreshes every 15s.

---

## API references

- **Adzuna** — https://developer.adzuna.com/docs/search
- **USAJobs** — https://developer.usajobs.gov/api-reference/get-api-search
- **RemoteOK** — https://remoteok.com/api (no key)
- **FastMCP** — https://gofastmcp.com/
- **Databricks MCP hosting** — https://docs.databricks.com/aws/en/agents/mcp-tools/custom-mcp
