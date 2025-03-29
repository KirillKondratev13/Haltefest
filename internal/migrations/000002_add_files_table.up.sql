CREATE TABLE user_files (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    seaweedfs_file_id VARCHAR(255) NOT NULL,  -- Идентификатор файла в SeaweedFS
    original_name TEXT NOT NULL,              -- Оригинальное имя файла
    size BIGINT NOT NULL,                     -- Размер файла в байтах
    mime_type TEXT,                           -- MIME-тип (например, "image/png")
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_user_files_user_id ON user_files(user_id);