-- Storage Plugin Tables
CREATE TABLE IF NOT EXISTS _plugin_storage_files (
    id SEQ PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    original_name VARCHAR(255),
    path VARCHAR(500) NOT NULL,
    size INT,
    mime_type VARCHAR(100),
    hash VARCHAR(64),
    user_id INT,
    metadata TEXT,
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_storage_filename ON _plugin_storage_files(filename);
CREATE INDEX IF NOT EXISTS idx_storage_user_id ON _plugin_storage_files(user_id);