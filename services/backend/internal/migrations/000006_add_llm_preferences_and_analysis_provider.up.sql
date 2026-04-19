ALTER TABLE analysis_jobs
    ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'local';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'analysis_jobs_provider_check'
    ) THEN
        ALTER TABLE analysis_jobs
            ADD CONSTRAINT analysis_jobs_provider_check
            CHECK (provider IN ('local', 'gigachat'));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_analysis_jobs_user_file_type_provider_status
    ON analysis_jobs (user_id, file_id, analysis_type, provider, status, created_at DESC);

CREATE TABLE IF NOT EXISTS user_llm_preferences (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    summary_provider TEXT NOT NULL DEFAULT 'local',
    chapters_provider TEXT NOT NULL DEFAULT 'local',
    flashcards_provider TEXT NOT NULL DEFAULT 'local',
    chat_default_provider TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (summary_provider IN ('local', 'gigachat')),
    CHECK (chapters_provider IN ('local', 'gigachat')),
    CHECK (flashcards_provider IN ('local', 'gigachat')),
    CHECK (chat_default_provider IN ('local', 'gigachat'))
);
