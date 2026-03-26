-- Logging Plugin Tables
CREATE TABLE IF NOT EXISTS _plugin_log_entries (
    id SEQ PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    source VARCHAR(100),
    message TEXT NOT NULL,
    data TEXT,
    created_at DATETIME DEFAULT NOW(),
    ip_address VARCHAR(50),
    user_id INT
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_log_level ON _plugin_log_entries(level);
CREATE INDEX IF NOT EXISTS idx_log_source ON _plugin_log_entries(source);
CREATE INDEX IF NOT EXISTS idx_log_created_at ON _plugin_log_entries(created_at);