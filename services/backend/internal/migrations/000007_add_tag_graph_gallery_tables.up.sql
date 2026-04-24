CREATE TABLE IF NOT EXISTS tags (
    id BIGSERIAL PRIMARY KEY,
    normalized_name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    is_system BOOLEAN NOT NULL DEFAULT FALSE,
    created_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (is_system OR char_length(display_name) <= 15)
);

CREATE TABLE IF NOT EXISTS file_tags (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source IN ('AUTO', 'MANUAL')),
    score DOUBLE PRECISION,
    auto_rank SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (file_id, tag_id),
    CHECK (
        (source = 'AUTO' AND auto_rank BETWEEN 1 AND 5)
        OR (source = 'MANUAL' AND auto_rank IS NULL)
    ),
    CHECK (score IS NULL OR (score >= 0.0 AND score <= 1.0))
);

CREATE INDEX IF NOT EXISTS idx_file_tags_file_source_rank ON file_tags (file_id, source, auto_rank);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag_source ON file_tags (tag_id, source);

CREATE TABLE IF NOT EXISTS user_graph_preferences (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    show_tag_probabilities BOOLEAN NOT NULL DEFAULT FALSE,
    gallery_visibility TEXT NOT NULL DEFAULT 'private' CHECK (gallery_visibility IN ('private', 'public')),
    gallery_snapshot_path TEXT,
    gallery_snapshot_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS graph_unique_views (
    id BIGSERIAL PRIMARY KEY,
    owner_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    viewer_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    viewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    view_date_utc DATE NOT NULL,
    UNIQUE (owner_user_id, viewer_user_id, view_date_utc)
);

CREATE INDEX IF NOT EXISTS idx_graph_unique_views_owner_date ON graph_unique_views (owner_user_id, view_date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_graph_unique_views_viewer_date ON graph_unique_views (viewer_user_id, view_date_utc DESC);
