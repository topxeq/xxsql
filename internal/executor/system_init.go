package executor

import (
	"fmt"

	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// System table names
const (
	SysTableMicroservices = "_sys_ms"
	SysTableProjects      = "_sys_projects"
)

// InitSystemTables initializes system tables for XxSql.
// This should be called when the server first starts.
func InitSystemTables(engine *storage.Engine) error {
	if engine == nil {
		return fmt.Errorf("engine is nil")
	}

	// Check if tables already exist
	if engine.TableExists(SysTableMicroservices) && engine.TableExists(SysTableProjects) {
		return nil // Already initialized
	}

	// Create _sys_ms table for microservice scripts
	if !engine.TableExists(SysTableMicroservices) {
		err := createSystemMSTable(engine)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", SysTableMicroservices, err)
		}
	}

	// Create _sys_projects table for installed projects
	if !engine.TableExists(SysTableProjects) {
		err := createSystemProjectsTable(engine)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", SysTableProjects, err)
		}
	}

	return nil
}

// InitSystemMicroservices inserts preset system microservice scripts
func InitSystemMicroservices(engine *storage.Engine, dataDir string) error {
	if engine == nil {
		return fmt.Errorf("engine is nil")
	}

	// File upload microservice
	uploadScript := `
var data = http.bodyJSON()
if (data == null || data.path == null || data.content == null) {
    http.status(400)
    http.json({"success": false, "error": "Missing path or content"})
} else {
    var result = fileSave(data.path, data.content, data.mode)
    http.json(result)
}
`
	if err := InsertSystemMicroservice(engine, "file/upload", uploadScript, "Upload file to server"); err != nil {
		return fmt.Errorf("failed to insert file/upload: %w", err)
	}

	// Binary file upload microservice
	uploadBinaryScript := `
var data = http.bodyJSON()
if (data == null || data.path == null || data.content == null) {
    http.status(400)
    http.json({"success": false, "error": "Missing path or content"})
} else {
    var result = fileSave(data.path, data.content, "binary")
    http.json(result)
}
`
	if err := InsertSystemMicroservice(engine, "file/uploadBinary", uploadBinaryScript, "Upload binary file to server"); err != nil {
		return fmt.Errorf("failed to insert file/uploadBinary: %w", err)
	}

	// File read microservice
	readScript := `
var path = http.param("path")
if (path == "") {
    http.status(400)
    http.json({"success": false, "error": "Missing path parameter"})
} else {
    var mode = http.param("mode")
    var result = fileRead(path, mode)
    http.json(result)
}
`
	if err := InsertSystemMicroservice(engine, "file/read", readScript, "Read file from server"); err != nil {
		return fmt.Errorf("failed to insert file/read: %w", err)
	}

	// File delete microservice
	deleteScript := `
var data = http.bodyJSON()
if (data == null || data.path == null) {
    http.status(400)
    http.json({"success": false, "error": "Missing path"})
} else {
    var result = fileDelete(data.path)
    http.json(result)
}
`
	if err := InsertSystemMicroservice(engine, "file/delete", deleteScript, "Delete file from server"); err != nil {
		return fmt.Errorf("failed to insert file/delete: %w", err)
	}

	// Directory list microservice
	listScript := `
var path = http.param("path")
if (path == "") {
    path = ""
}
var result = dirList(path)
http.json({"success": true, "files": result})
`
	if err := InsertSystemMicroservice(engine, "dir/list", listScript, "List directory contents"); err != nil {
		return fmt.Errorf("failed to insert dir/list: %w", err)
	}

	// Directory create microservice
	mkdirScript := `
var data = http.bodyJSON()
if (data == null || data.path == null) {
    http.status(400)
    http.json({"success": false, "error": "Missing path"})
} else {
    var result = dirCreate(data.path)
    http.json(result)
}
`
	if err := InsertSystemMicroservice(engine, "dir/create", mkdirScript, "Create directory on server"); err != nil {
		return fmt.Errorf("failed to insert dir/create: %w", err)
	}

	// Project check microservice
	projectCheckScript := `
var data = http.bodyJSON()
if (data == null || data.name == null) {
    http.status(400)
    http.json({"error": "Missing name"})
} else {
    var result = db.queryRow("SELECT name FROM _sys_projects WHERE name = '" + data.name + "'")
    if (result == null) {
        http.json({"exists": false, "name": data.name})
    } else {
        http.json({"exists": true, "name": data.name})
    }
}
`
	if err := InsertSystemMicroservice(engine, "project/check", projectCheckScript, "Check if project is installed"); err != nil {
		return fmt.Errorf("failed to insert project/check: %w", err)
	}

	// Project register microservice
	projectRegisterScript := `
var data = http.bodyJSON()
if (data == null || data.name == null) {
    http.status(400)
    http.json({"success": false, "error": "Missing name"})
} else {
    var result = db.exec("INSERT INTO _sys_projects (name, version, installed_at, tables) VALUES ('" + data.name + "', '" + string(data.version) + "', datetime('now'), '" + string(data.tables) + "')")
    http.json({"success": true, "affected": result.affected})
}
`
	if err := InsertSystemMicroservice(engine, "project/register", projectRegisterScript, "Register installed project"); err != nil {
		return fmt.Errorf("failed to insert project/register: %w", err)
	}

	// Health check microservice
	healthScript := `
http.json({"status": "ok", "service": "xxsql"})
`
	if err := InsertSystemMicroservice(engine, "health", healthScript, "Health check endpoint"); err != nil {
		return fmt.Errorf("failed to insert health: %w", err)
	}

	// Static file serve microservice
	serveScript := `
var path = http.param("path")
if (path == "") {
    http.status(400)
    http.json({"success": false, "error": "Missing path parameter"})
} else {
    var result = fileServe(path)
    if (result.success == false) {
        http.status(404)
        http.json(result)
    }
}
`
	if err := InsertSystemMicroservice(engine, "file/serve", serveScript, "Serve static file with proper content type"); err != nil {
		return fmt.Errorf("failed to insert file/serve: %w", err)
	}

	return nil
}

func createSystemMSTable(engine *storage.Engine) error {
	columns := []*types.ColumnInfo{
		{
			Name:       "SKEY",
			Type:       types.TypeVarchar,
			Size:       100,
			Nullable:   false,
			PrimaryKey: true,
		},
		{
			Name:     "SCRIPT",
			Type:     types.TypeText,
			Nullable: true,
		},
		{
			Name:     "description",
			Type:     types.TypeVarchar,
			Size:     255,
			Nullable: true,
		},
		{
			Name:     "created_at",
			Type:     types.TypeDatetime,
			Nullable: true,
		},
	}

	return engine.CreateTable(SysTableMicroservices, columns)
}

func createSystemProjectsTable(engine *storage.Engine) error {
	columns := []*types.ColumnInfo{
		{
			Name:       "name",
			Type:       types.TypeVarchar,
			Size:       100,
			Nullable:   false,
			PrimaryKey: true,
		},
		{
			Name:     "version",
			Type:     types.TypeVarchar,
			Size:     20,
			Nullable: true,
		},
		{
			Name:     "installed_at",
			Type:     types.TypeDatetime,
			Nullable: true,
		},
		{
			Name:     "tables",
			Type:     types.TypeText,
			Nullable: true,
		},
	}

	return engine.CreateTable(SysTableProjects, columns)
}

// InsertSystemMicroservice inserts a microservice script into _sys_ms table
func InsertSystemMicroservice(engine *storage.Engine, skey, script, description string) error {
	exec := NewExecutor(engine)

	// Check if already exists
	result, err := exec.Execute(fmt.Sprintf("SELECT SKEY FROM %s WHERE SKEY = '%s'", SysTableMicroservices, skey))
	if err == nil && len(result.Rows) > 0 {
		// Update existing
		_, err = exec.Execute(fmt.Sprintf("UPDATE %s SET SCRIPT = '%s', description = '%s' WHERE SKEY = '%s'",
			SysTableMicroservices, escapeSQLString(script), escapeSQLString(description), skey))
		return err
	}

	// Insert new
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO %s (SKEY, SCRIPT, description) VALUES ('%s', '%s', '%s')",
		SysTableMicroservices, skey, escapeSQLString(script), escapeSQLString(description)))
	return err
}

// escapeSQLString escapes single quotes in SQL strings
func escapeSQLString(s string) string {
	result := ""
	for _, c := range s {
		if c == '\'' {
			result += "''"
		} else {
			result += string(c)
		}
	}
	return result
}