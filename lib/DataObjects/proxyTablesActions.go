package DataObjects

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
	"os"
)
/*
create table
 */
func (pnode ProxySQLNode) executeProxyCommand(connection *sql.DB, command string) bool{

	if connection != nil &&
			command != "" {

		result, err  := connection.Exec(command)
		if err != nil {
			log.Error(err.Error())
			os.Exit(1)
		}
		rows, err := result.RowsAffected()
		if int64(-1) < rows && err == nil {
			if log.GetLevel() == log.DebugLevel{
				log.Debug("Executing query: ", command)
			}
			return true
		}
		if err != nil{
			log.Error(err.Error())
			os.Exit(1)
		}

	}

	return false
}
