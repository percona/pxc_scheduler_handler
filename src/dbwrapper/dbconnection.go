//go:generate mockgen -source dbconnection.go -destination mock/dbconnection_mock.go -package mock
package dbwrapper

import (
	"context"
	"database/sql"
)

// DBConnection is the interface for database connection
type DBConnection interface {
	BeginTx(context.Context) (DBTransaction, error)
	Exec(query string, args ...interface{}) error
	Query(query string, args ...interface{}) (DBRows, error)
	Ping() error
	Close() error
}

// NewDBConnection creates database connection object
func NewDBConnection(driverName string, dataSourceName string) (DBConnection, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	connection := dbConnection{
		connection: db,
	}

	return &connection, nil
}

type dbConnection struct {
	connection *sql.DB
}

func (c *dbConnection) BeginTx(ctx context.Context) (DBTransaction, error) {
	t, err := c.connection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	trx := dbTransaction{
		transaction: t,
	}

	return &trx, nil
}

func (c *dbConnection) Exec(query string, args ...interface{}) error {
	_, err := c.connection.Exec(query, args...)
	return err
}

func (c *dbConnection) Query(query string, args ...interface{}) (DBRows, error) {
	r, err := c.connection.Query(query, args...)
	if err != nil {
		return nil, err
	}
	rows := dbRows{
		rows: r,
	}

	return &rows, nil
}

func (c *dbConnection) Ping() error {
	return c.connection.Ping()
}

func (c *dbConnection) Close() error {
	return c.connection.Close()
}
