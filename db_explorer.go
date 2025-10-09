package main

import (
	"database/sql"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type DbExplorerHandler struct {
}

func (handler DbExplorerHandler) ServeHTTP(http.ResponseWriter, *http.Request) {

}

func NewDbExplorer(db *sql.DB) (*DbExplorerHandler, error) {
	return &DbExplorerHandler{}, nil
}

func GetAllTableNames(db *sql.DB) ([]string, error) {
	return []string{}, nil
}
