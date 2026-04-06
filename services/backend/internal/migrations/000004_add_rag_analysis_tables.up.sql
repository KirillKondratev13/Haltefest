CREATE TABLE IF NOT EXISTS text_artifacts (
    id BIGSERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    s3_text_path TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    text_version INT NOT NULL,
    hash_sha256 TEXT NOT NULL,
    index_status VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
    index_error TEXT,
    indexed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (file_id, text_version),
    CHECK (text_version > 0),
    CHECK (index_status IN ('QUEUED', 'PROCESSING', 'DONE', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_text_artifacts_user_file ON text_artifacts (user_id, file_id);
CREATE INDEX IF NOT EXISTS idx_text_artifacts_index_status ON text_artifacts (index_status, created_at);

CREATE TABLE IF NOT EXISTS analysis_jobs (
    id BIGSERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    analysis_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    error TEXT,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (analysis_type IN ('summary', 'chapters', 'flashcards')),
    CHECK (status IN ('QUEUED', 'PROCESSING', 'DONE', 'FAILED')),
    CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_analysis_jobs_file_type ON analysis_jobs (file_id, analysis_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analysis_jobs_status ON analysis_jobs (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_analysis_jobs_user ON analysis_jobs (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS analysis_results (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES analysis_jobs(id) ON DELETE CASCADE UNIQUE,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    result_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    token_usage_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_results_file ON analysis_results (file_id, created_at DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_type TEXT NOT NULL,
    aggregate_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'NEW',
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    CHECK (status IN ('NEW', 'SENT', 'FAILED')),
    CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox_events (status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_event_type_status ON outbox_events (event_type, status, created_at);
