-- =====================================================================
-- AI Job Hunting Copilot - Lakebase Schema
-- =====================================================================
-- Run these statements manually against your Databricks Lakebase Postgres
-- database (via psql, DBeaver, or a Databricks SQL notebook connected
-- to the Lakebase instance). Requires the pgvector extension for
-- semantic search over job descriptions.
-- =====================================================================

-- Vector extension (required for semantic search)
CREATE EXTENSION IF NOT EXISTS vector;


-- ---------------------------------------------------------------------
-- 1. USERS: identity + high-level job search preferences
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    email             TEXT PRIMARY KEY,
    name              TEXT,
    target_role       TEXT,
    target_locations  TEXT[],
    remote_ok         BOOLEAN DEFAULT true,
    salary_min        NUMERIC,
    currency          TEXT DEFAULT 'USD',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- 2. PROFILES: unstructured resume + structured skills
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS profiles (
    email             TEXT PRIMARY KEY REFERENCES users(email) ON DELETE CASCADE,
    resume_text       TEXT,
    skills            TEXT[],
    years_experience  INTEGER,
    seniority         TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- 3. JOB_POSTINGS: raw job data from Adzuna / USAJobs / RemoteOK
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_postings (
    id            TEXT PRIMARY KEY,
    source        TEXT NOT NULL CHECK (source IN ('adzuna', 'usajobs', 'remoteok')),
    external_id   TEXT NOT NULL,
    title         TEXT NOT NULL,
    company       TEXT,
    location      TEXT,
    remote        BOOLEAN DEFAULT false,
    salary_min    NUMERIC,
    salary_max    NUMERIC,
    currency      TEXT DEFAULT 'USD',
    description   TEXT,
    url           TEXT,
    category      TEXT,
    posted_at     TIMESTAMPTZ,
    payload       JSONB NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_postings_source     ON job_postings (source);
CREATE INDEX IF NOT EXISTS idx_job_postings_posted_at  ON job_postings (posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_postings_company    ON job_postings (company);


-- ---------------------------------------------------------------------
-- 4. JOB_EMBEDDINGS: pgvector semantic search over descriptions
-- ---------------------------------------------------------------------
-- sentence-transformers/all-MiniLM-L6-v2 outputs 384-dimensional vectors
CREATE TABLE IF NOT EXISTS job_embeddings (
    job_id      TEXT PRIMARY KEY REFERENCES job_postings(id) ON DELETE CASCADE,
    embedding   vector(384),
    model_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_embeddings_vec
    ON job_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- ---------------------------------------------------------------------
-- 5. APPLICATIONS: user pipeline (saved -> applied -> interviewing ...)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS applications (
    id                SERIAL PRIMARY KEY,
    email             TEXT NOT NULL,
    job_id            TEXT NOT NULL REFERENCES job_postings(id) ON DELETE CASCADE,
    stage             TEXT NOT NULL CHECK (stage IN ('saved', 'applied', 'interviewing', 'offer', 'rejected')),
    match_score       NUMERIC,
    match_reasoning   TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (email, job_id)
);

CREATE INDEX IF NOT EXISTS idx_applications_email  ON applications (email);
CREATE INDEX IF NOT EXISTS idx_applications_stage  ON applications (stage);


-- ---------------------------------------------------------------------
-- 6. INTERVIEW_NOTES: unstructured notes tied to applications
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS interview_notes (
    id              SERIAL PRIMARY KEY,
    application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    email           TEXT NOT NULL,
    note            TEXT NOT NULL,
    interview_date  DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_interview_notes_email ON interview_notes (email);


-- ---------------------------------------------------------------------
-- 7. COVER_LETTERS: agent-generated tailored cover letters
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cover_letters (
    id                 SERIAL PRIMARY KEY,
    email              TEXT NOT NULL,
    job_id             TEXT NOT NULL REFERENCES job_postings(id) ON DELETE CASCADE,
    cover_letter_text  TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cover_letters_email ON cover_letters (email);


-- ---------------------------------------------------------------------
-- 8. SAVED_SEARCHES: track user queries + filters for analytics
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saved_searches (
    id             SERIAL PRIMARY KEY,
    email          TEXT NOT NULL,
    query          TEXT NOT NULL,
    filters        JSONB,
    results_count  INTEGER,
    sources        TEXT[],
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_saved_searches_email ON saved_searches (email);


-- ---------------------------------------------------------------------
-- 9. AGENT_ACTIVITY_LOG: trace EVERY MCP tool call for the dashboard feed
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_activity_log (
    id              SERIAL PRIMARY KEY,
    email           TEXT,
    tool_name       TEXT NOT NULL,
    params          JSONB,
    result_summary  TEXT,
    status          TEXT NOT NULL DEFAULT 'success',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_activity_log_email       ON agent_activity_log (email);
CREATE INDEX IF NOT EXISTS idx_agent_activity_log_created_at  ON agent_activity_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_activity_log_tool_name   ON agent_activity_log (tool_name);


-- ---------------------------------------------------------------------
-- Verification query - run after creating tables
-- ---------------------------------------------------------------------
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'public'
--   AND table_name IN (
--       'users', 'profiles', 'job_postings', 'job_embeddings',
--       'applications', 'interview_notes', 'cover_letters',
--       'saved_searches', 'agent_activity_log'
--   )
-- ORDER BY table_name;
