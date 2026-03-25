package plugin

// Registry contains information about available official plugins.
// This is the built-in registry that can be updated from GitHub.

// RegistryPlugin represents a plugin in the registry.
type RegistryPlugin struct {
	Name        string     `json:"name"`
	Version     string     `json:"version"`
	Author      string     `json:"author"`
	Description string     `json:"description"`
	Category    string     `json:"category"`
	Tables      string     `json:"tables"`
	Endpoints   []Endpoint `json:"endpoints"`
	DownloadURL string     `json:"download_url"`
}

// OfficialPlugins is the built-in registry of official XxSql plugins.
var OfficialPlugins = []RegistryPlugin{
	{
		Name:        "auth",
		Version:     "1.0.0",
		Author:      "XxSql Team",
		Description: "User authentication with session management, login/register/logout endpoints",
		Category:    "auth",
		Tables:      "_plugin_auth_users,_plugin_auth_sessions",
		Endpoints: []Endpoint{
			{SKEY: "auth/login", Description: "User login"},
			{SKEY: "auth/register", Description: "Register new user"},
			{SKEY: "auth/logout", Description: "User logout"},
			{SKEY: "auth/check", Description: "Check authentication status"},
			{SKEY: "auth/user", Description: "Get current user info"},
		},
		DownloadURL: "https://github.com/topxeq/xxsql-plugins/raw/main/auth.zip",
	},
	{
		Name:        "logging",
		Version:     "1.0.0",
		Author:      "XxSql Team",
		Description: "Centralized logging service with query and filtering capabilities",
		Category:    "logging",
		Tables:      "_plugin_log_entries",
		Endpoints: []Endpoint{
			{SKEY: "log/write", Description: "Write log entry"},
			{SKEY: "log/query", Description: "Query logs with filters"},
			{SKEY: "log/clear", Description: "Clear old logs"},
			{SKEY: "log/stats", Description: "Log statistics"},
		},
		DownloadURL: "https://github.com/topxeq/xxsql-plugins/raw/main/logging.zip",
	},
	{
		Name:        "ratelimit",
		Version:     "1.0.0",
		Author:      "XxSql Team",
		Description: "Request rate limiting with configurable rules per IP or user",
		Category:    "utility",
		Tables:      "_plugin_ratelimit_rules,_plugin_ratelimit_counters",
		Endpoints: []Endpoint{
			{SKEY: "ratelimit/check", Description: "Check if request is allowed"},
			{SKEY: "ratelimit/rules", Description: "Manage rate limit rules"},
			{SKEY: "ratelimit/reset", Description: "Reset rate limit counter"},
		},
		DownloadURL: "https://github.com/topxeq/xxsql-plugins/raw/main/ratelimit.zip",
	},
	{
		Name:        "storage",
		Version:     "1.0.0",
		Author:      "XxSql Team",
		Description: "File storage service with upload, download, and management",
		Category:    "storage",
		Tables:      "_plugin_storage_files",
		Endpoints: []Endpoint{
			{SKEY: "storage/upload", Description: "Upload file"},
			{SKEY: "storage/download", Description: "Download file"},
			{SKEY: "storage/list", Description: "List files"},
			{SKEY: "storage/delete", Description: "Delete file"},
			{SKEY: "storage/info", Description: "Get file info"},
		},
		DownloadURL: "https://github.com/topxeq/xxsql-plugins/raw/main/storage.zip",
	},
}

// GetAvailablePlugins returns plugins available in the registry.
// For now, returns the built-in list. In future, can fetch from GitHub.
func GetAvailablePlugins() []RegistryPlugin {
	return OfficialPlugins
}

// GetRegistryPlugin returns a specific plugin from the registry.
func GetRegistryPlugin(name string) *RegistryPlugin {
	for _, p := range OfficialPlugins {
		if p.Name == name {
			return &p
		}
	}
	return nil
}

// PluginCategories returns the list of plugin categories.
var PluginCategories = []string{
	"auth",
	"logging",
	"storage",
	"utility",
}