CREATE TABLE user_files (
	    id SERIAL PRIMARY KEY,
	    user_id INTEGER NOT NULL,
	    seaweedfs_file_id VARCHAR(255) NOT NULL,
	    original_name TEXT NOT NULL,
	    size BIGINT NOT NULL,
	    mime_type TEXT,
	    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
	    CONSTRAINT user_files_user_id_fkey FOREIGN KEY (user_id) 
	        REFERENCES users(id) ON DELETE CASCADE
	);

	CREATE INDEX idx_user_files_user_id ON user_files(user_id);
