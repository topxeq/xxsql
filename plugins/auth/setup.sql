-- Auth Plugin Tables
-- Users table
CREATE TABLE IF NOT EXISTS _plugin_auth_users (
    id SEQ PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) DEFAULT 'user',
    created_at DATETIME DEFAULT NOW(),
    last_login DATETIME,
    enabled BOOL DEFAULT TRUE
);

-- Sessions table
CREATE TABLE IF NOT EXISTS _plugin_auth_sessions (
    id SEQ PRIMARY KEY,
    user_id INT NOT NULL,
    token VARCHAR(255) UNIQUE NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at DATETIME DEFAULT NOW(),
    ip_address VARCHAR(50),
    user_agent TEXT
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_auth_users_username ON _plugin_auth_users(username);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_token ON _plugin_auth_sessions(token);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_id ON _plugin_auth_sessions(user_id);