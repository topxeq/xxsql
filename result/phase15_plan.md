# Phase 15: Backup/Recovery Implementation Plan

## Overview
Implement full backup and restore functionality for XxSql database, allowing users to create consistent backups and restore from them.

## Sub-phases

### Phase 15.1: Backup Module Core
- Create `internal/backup/backup.go` module
- Define backup format (tar archive with metadata)
- Implement full backup functionality
- Backup components:
  - Table metadata (.xmeta files)
  - Table data (.xdb files)
  - Index files (.xidx files)
  - Sequence data (.seq files)
  - Backup manifest (JSON metadata)

### Phase 15.2: SQL Syntax
- Add tokens: `TokBackup`, `TokRestore`
- Add AST nodes: `BackupStmt`, `RestoreStmt`
- Parser support for:
  - `BACKUP DATABASE TO 'path'`
  - `BACKUP DATABASE TO 'path' WITH COMPRESS`
  - `RESTORE DATABASE FROM 'path'`

### Phase 15.3: Restore Functionality
- Implement restore from backup archive
- Validate backup integrity
- Restore all tables, indexes, and sequences

### Phase 15.4: Integration
- Add executor methods for backup/restore
- Integrate with storage engine
- Permission checks for backup operations

## Backup File Format

```
backup.xbak (tar archive)
├── manifest.json          # Backup metadata
├── tables/
│   ├── users.xmeta       # Table metadata
│   ├── users.xdb         # Table data
│   ├── users.xidx        # Index files (if any)
│   └── ...
├── sequences/
│   └── _sequences.seq    # Sequence data
└── auth/
    ├── users.json        # User data
    └── grants.json       # Permission grants
```

## Backup Manifest Structure

```json
{
  "version": "1.0",
  "timestamp": "2026-03-16T10:00:00Z",
  "database": "default",
  "table_count": 5,
  "tables": [
    {
      "name": "users",
      "row_count": 100,
      "page_count": 10
    }
  ],
  "checksum": "sha256:..."
}
```

## Estimated Effort
- Phase 15.1: 2-3 hours (backup module)
- Phase 15.2: 1 hour (SQL syntax)
- Phase 15.3: 2 hours (restore)
- Phase 15.4: 1 hour (integration)
- Total: 6-7 hours

## Dependencies
- Storage engine (Phase 3)
- WAL (Phase 5)
- Checkpoint (Phase 5)
- Auth (Phase 10)

## Implementation Order
1. Create backup module with backup format
2. Add SQL tokens and AST nodes
3. Update parser
4. Implement executor methods
5. Add tests
