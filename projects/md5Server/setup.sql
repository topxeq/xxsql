-- md5Server Setup Script
-- This script sets up the MD5 hash generation microservice

-- Create the microservice API table
CREATE TABLE IF NOT EXISTS api (
    SKEY VARCHAR(50) PRIMARY KEY,
    SCRIPT TEXT
);

-- MD5 hash generation endpoint
-- Usage: POST /ms/api/md5 with JSON body {"text": "your text here"}
-- Returns: {"hash": "md5_hash_value"}
INSERT INTO api (SKEY, SCRIPT) VALUES ('md5', '
var data = http.bodyJSON()
if (data == null || data.text == null) {
    http.status(400)
    http.json({"error": "Missing text parameter"})
} else {
    var hash = md5(data.text)
    http.json({"hash": hash, "text": data.text})
}
');

-- Health check endpoint
-- Usage: GET /ms/api/health
-- Returns: {"status": "ok"}
INSERT INTO api (SKEY, SCRIPT) VALUES ('health', '
http.json({"status": "ok", "service": "md5Server"})
');