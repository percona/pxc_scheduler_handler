//go:generate mockgen -source dbtransaction.go -destination mock/dbtransaction_mock.go -package mock
package dbwrapper

import (
	"context"
	"database/sql"
)

// DBTransaction is the interface for database transaction
type DBTransaction interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) error
	Rollback() error
	Commit() error
}

type dbTransaction struct {
	transaction *sql.Tx
}

func (t *dbTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) error {
	_, err := t.transaction.ExecContext(ctx, query, args...)
	return err
}

func (t *dbTransaction) Rollback() error {
	return t.transaction.Rollback()
}

func (t *dbTransaction) Commit() error {
	return t.transaction.Commit()
}
