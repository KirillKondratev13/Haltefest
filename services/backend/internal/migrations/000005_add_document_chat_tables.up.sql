CREATE TABLE IF NOT EXISTS chat_threads (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title TEXT,
    scope_mode VARCHAR(20) NOT NULL, -- single-doc|multi-doc|all-docs
    selected_file_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_provider TEXT NOT NULL DEFAULT 'local', -- local|gigachat
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (scope_mode IN ('single-doc', 'multi-doc', 'all-docs')),
    CHECK (jsonb_typeof(selected_file_ids_json) = 'array'),
    CHECK (default_provider IN ('local', 'gigachat'))
);

CREATE INDEX IF NOT EXISTS idx_chat_threads_user_created ON chat_threads (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_threads_user_updated ON chat_threads (user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_threads_scope_mode ON chat_threads (scope_mode, created_at DESC);

CREATE TABLE IF NOT EXISTS chat_messages (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES chat_threads(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL, -- user|assistant|system
    content TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (role IN ('user', 'assistant', 'system'))
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_created ON chat_messages (chat_id, created_at);
CREATE INDEX IF NOT EXISTS idx_chat_messages_user_created ON chat_messages (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS chat_jobs (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES chat_threads(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    question_message_id BIGINT NOT NULL REFERENCES chat_messages(id) ON DELETE CASCADE,
    assistant_message_id BIGINT REFERENCES chat_messages(id) ON DELETE SET NULL,
    status VARCHAR(20) NOT NULL, -- QUEUED|PROCESSING|DONE|FAILED
    provider TEXT NOT NULL DEFAULT 'local', -- local|gigachat
    routing_mode VARCHAR(20) NOT NULL DEFAULT 'AUTO', -- AUTO|FULL_CONTEXT|RAG
    scope_mode VARCHAR(20) NOT NULL, -- single-doc|multi-doc|all-docs
    selected_file_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    threshold_tokens INTEGER,
    attempts INT NOT NULL DEFAULT 0,
    error TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status IN ('QUEUED', 'PROCESSING', 'DONE', 'FAILED')),
    CHECK (provider IN ('local', 'gigachat')),
    CHECK (routing_mode IN ('AUTO', 'FULL_CONTEXT', 'RAG')),
    CHECK (scope_mode IN ('single-doc', 'multi-doc', 'all-docs')),
    CHECK (jsonb_typeof(selected_file_ids_json) = 'array'),
    CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_chat_jobs_user_status_requested ON chat_jobs (user_id, status, requested_at);
CREATE INDEX IF NOT EXISTS idx_chat_jobs_chat_created ON chat_jobs (chat_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_jobs_scope_provider ON chat_jobs (scope_mode, provider, created_at DESC);
