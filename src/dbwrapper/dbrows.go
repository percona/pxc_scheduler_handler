//go:generate mockgen -source dbrows.go -destination mock/dbrows_mock.go -package mock
package dbwrapper

import "database/sql"

// DBRows is the interface for query result rows set
type DBRows interface {
	Next() bool
	Scan(dest ...interface{}) error
}

type dbRows struct {
	rows *sql.Rows
}

func (r *dbRows) Next() bool {
	return r.rows.Next()
}

func (r *dbRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}
