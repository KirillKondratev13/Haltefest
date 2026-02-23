CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    file_type TEXT NOT NULL,
    status VARCHAR(20),     -- PENDING, PROCESSING, READY, ERROR (NULL if not for processing)
    tag VARCHAR(50),        -- Predicted tag
    error_msg TEXT,         -- Processing error details
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);