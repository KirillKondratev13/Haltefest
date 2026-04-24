CREATE TABLE IF NOT EXISTS graph_file_downloads (
    id BIGSERIAL PRIMARY KEY,
    owner_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    viewer_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    downloaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    download_date_utc DATE NOT NULL,
    UNIQUE (owner_user_id, viewer_user_id, file_id, download_date_utc)
);

CREATE INDEX IF NOT EXISTS idx_graph_file_downloads_owner_date
    ON graph_file_downloads (owner_user_id, download_date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_graph_file_downloads_viewer_date
    ON graph_file_downloads (viewer_user_id, download_date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_graph_file_downloads_file_date
    ON graph_file_downloads (file_id, download_date_utc DESC);
