package xxsql

// tx implements driver.Tx.
type tx struct {
	conn   *conn
	closed bool
}

// Commit commits the transaction.
func (t *tx) Commit() error {
	if t.closed {
		return ErrTxDone
	}
	t.closed = true
	t.conn.inTx = false

	_, _, err := t.conn.mysqlConn.exec("COMMIT")
	return err
}

// Rollback rolls back the transaction.
func (t *tx) Rollback() error {
	if t.closed {
		return ErrTxDone
	}
	t.closed = true
	t.conn.inTx = false

	_, _, err := t.conn.mysqlConn.exec("ROLLBACK")
	return err
}

// result implements driver.Result.
type result struct {
	affectedRows int64
	lastInsertID int64
}

// LastInsertId returns the last inserted ID.
func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of affected rows.
func (r *result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}
