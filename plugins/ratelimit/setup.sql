-- Rate Limit Plugin Tables

-- Rules table
CREATE TABLE IF NOT EXISTS _plugin_ratelimit_rules (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    key_pattern VARCHAR(255) NOT NULL,
    max_requests INT NOT NULL DEFAULT 100,
    window_seconds INT NOT NULL DEFAULT 60,
    action VARCHAR(20) DEFAULT 'block',
    enabled BOOL DEFAULT TRUE,
    created_at DATETIME DEFAULT NOW()
);

-- Counters table
CREATE TABLE IF NOT EXISTS _plugin_ratelimit_counters (
    id SEQ PRIMARY KEY,
    key VARCHAR(255) NOT NULL,
    count INT DEFAULT 0,
    window_start DATETIME NOT NULL,
    expires_at DATETIME NOT NULL
);

-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_ratelimit_counters_key ON _plugin_ratelimit_counters(key);
CREATE INDEX IF NOT EXISTS idx_ratelimit_rules_key_pattern ON _plugin_ratelimit_rules(key_pattern);