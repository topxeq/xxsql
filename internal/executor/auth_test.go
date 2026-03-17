package executor

import (
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
)

func TestNewSessionPermissionAdapter(t *testing.T) {
	checker := func(perm uint32) bool {
		return perm == uint32(PermSelect)
	}
	adapter := NewSessionPermissionAdapter(checker)

	if !adapter.HasPermission(PermSelect) {
		t.Error("expected HasPermission(PermSelect) to return true")
	}
	if adapter.HasPermission(PermInsert) {
		t.Error("expected HasPermission(PermInsert) to return false")
	}
}

func TestConvertPrivileges(t *testing.T) {
	tests := []struct {
		name       string
		privs      []*sql.Privilege
		wantSelect bool
		wantInsert bool
		wantUpdate bool
		wantDelete bool
		wantCreate bool
		wantDrop   bool
		wantIndex  bool
		wantAlter  bool
	}{
		{
			name:       "all privileges",
			privs:      []*sql.Privilege{{Type: sql.PrivAll}},
			wantSelect: true,
			wantInsert: true,
			wantUpdate: true,
			wantDelete: true,
			wantCreate: true,
			wantDrop:   true,
			wantIndex:  true,
			wantAlter:  true,
		},
		{
			name:       "select only",
			privs:      []*sql.Privilege{{Type: sql.PrivSelect}},
			wantSelect: true,
		},
		{
			name:       "multiple privileges",
			privs:      []*sql.Privilege{{Type: sql.PrivSelect}, {Type: sql.PrivInsert}, {Type: sql.PrivUpdate}},
			wantSelect: true,
			wantInsert: true,
			wantUpdate: true,
		},
		{
			name:       "insert privilege",
			privs:      []*sql.Privilege{{Type: sql.PrivInsert}},
			wantInsert: true,
		},
		{
			name:       "update privilege",
			privs:      []*sql.Privilege{{Type: sql.PrivUpdate}},
			wantUpdate: true,
		},
		{
			name:       "delete privilege",
			privs:      []*sql.Privilege{{Type: sql.PrivDelete}},
			wantDelete: true,
		},
		{
			name:       "create privilege",
			privs:      []*sql.Privilege{{Type: sql.PrivCreate}},
			wantCreate: true,
		},
		{
			name:     "drop privilege",
			privs:    []*sql.Privilege{{Type: sql.PrivDrop}},
			wantDrop: true,
		},
		{
			name:      "index privilege",
			privs:     []*sql.Privilege{{Type: sql.PrivIndex}},
			wantIndex: true,
		},
		{
			name:      "alter privilege",
			privs:     []*sql.Privilege{{Type: sql.PrivAlter}},
			wantAlter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertPrivileges(tt.privs)
			if result.Select != tt.wantSelect {
				t.Errorf("Select = %v, want %v", result.Select, tt.wantSelect)
			}
			if result.Insert != tt.wantInsert {
				t.Errorf("Insert = %v, want %v", result.Insert, tt.wantInsert)
			}
			if result.Update != tt.wantUpdate {
				t.Errorf("Update = %v, want %v", result.Update, tt.wantUpdate)
			}
			if result.Delete != tt.wantDelete {
				t.Errorf("Delete = %v, want %v", result.Delete, tt.wantDelete)
			}
			if result.Create != tt.wantCreate {
				t.Errorf("Create = %v, want %v", result.Create, tt.wantCreate)
			}
			if result.Drop != tt.wantDrop {
				t.Errorf("Drop = %v, want %v", result.Drop, tt.wantDrop)
			}
			if result.Index != tt.wantIndex {
				t.Errorf("Index = %v, want %v", result.Index, tt.wantIndex)
			}
			if result.Alter != tt.wantAlter {
				t.Errorf("Alter = %v, want %v", result.Alter, tt.wantAlter)
			}
		})
	}
}

func TestExecutorWithoutAuthManager(t *testing.T) {
	exec := &Executor{}

	// All auth operations should fail without auth manager
	_, err := exec.executeCreateUser(&sql.CreateUserStmt{Username: "test", Identified: "pass"})
	if err == nil {
		t.Error("expected error for executeCreateUser without auth manager")
	}

	_, err = exec.executeDropUser(&sql.DropUserStmt{Username: "test"})
	if err == nil {
		t.Error("expected error for executeDropUser without auth manager")
	}

	_, err = exec.executeAlterUser(&sql.AlterUserStmt{Username: "test", Identified: "pass"})
	if err == nil {
		t.Error("expected error for executeAlterUser without auth manager")
	}

	_, err = exec.executeSetPassword(&sql.SetPasswordStmt{ForUser: "test", Password: "pass"})
	if err == nil {
		t.Error("expected error for executeSetPassword without auth manager")
	}

	_, err = exec.executeGrant(&sql.GrantStmt{To: "test"})
	if err == nil {
		t.Error("expected error for executeGrant without auth manager")
	}

	_, err = exec.executeRevoke(&sql.RevokeStmt{From: "test"})
	if err == nil {
		t.Error("expected error for executeRevoke without auth manager")
	}

	_, err = exec.executeShowGrants(&sql.ShowGrantsStmt{ForUser: "test"})
	if err == nil {
		t.Error("expected error for executeShowGrants without auth manager")
	}
}

func TestPermissionConstants(t *testing.T) {
	// Verify permission constants have expected values (bit shifts)
	// PermManageUsers = 1 << 0 = 1
	// PermManageConfig = 1 << 1 = 2
	// PermStartStop = 1 << 2 = 4
	// PermCreateTable = 1 << 3 = 8
	// PermDropTable = 1 << 4 = 16
	// PermCreateDatabase = 1 << 5 = 32
	// PermDropDatabase = 1 << 6 = 64
	// PermSelect = 1 << 7 = 128
	// PermInsert = 1 << 8 = 256
	// PermUpdate = 1 << 9 = 512
	// PermDelete = 1 << 10 = 1024
	// PermCreateIndex = 1 << 11 = 2048
	// PermDropIndex = 1 << 12 = 4096
	// PermBackup = 1 << 13 = 8192
	// PermRestore = 1 << 14 = 16384

	if PermManageUsers != 1 {
		t.Errorf("PermManageUsers = %d, want 1", PermManageUsers)
	}
	if PermManageConfig != 2 {
		t.Errorf("PermManageConfig = %d, want 2", PermManageConfig)
	}
	if PermStartStop != 4 {
		t.Errorf("PermStartStop = %d, want 4", PermStartStop)
	}
	if PermCreateTable != 8 {
		t.Errorf("PermCreateTable = %d, want 8", PermCreateTable)
	}
	if PermDropTable != 16 {
		t.Errorf("PermDropTable = %d, want 16", PermDropTable)
	}
	if PermSelect != 128 {
		t.Errorf("PermSelect = %d, want 128", PermSelect)
	}
	if PermInsert != 256 {
		t.Errorf("PermInsert = %d, want 256", PermInsert)
	}
	if PermUpdate != 512 {
		t.Errorf("PermUpdate = %d, want 512", PermUpdate)
	}
	if PermDelete != 1024 {
		t.Errorf("PermDelete = %d, want 1024", PermDelete)
	}
	if PermBackup != 8192 {
		t.Errorf("PermBackup = %d, want 8192", PermBackup)
	}
	if PermRestore != 16384 {
		t.Errorf("PermRestore = %d, want 16384", PermRestore)
	}
}