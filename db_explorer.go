package main

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type DbExplorer struct {
	db *sql.DB
}

/* TODO:

 */

//
//

//
//

func (e *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// r.URL.Path - Путь (часть после хоста) = "/users/5"
	path := strings.Trim(r.URL.Path, "/") // на случай если "/users/5/ "
	parts := strings.Split(path, "/")

	switch r.Method {
	case "GET":
		if path == "" {
			tableNames, err := e.getAllTableNames()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			for _, tableName := range tableNames {
				_, err := w.Write([]byte(tableName))
				if err != nil {
					return
				}
			}
		}

		// GET /$table?limit=5&offset=7
		if len(parts) == 1 {
			params := r.URL.Query()
			limit, _ := strconv.Atoi(params.Get("limit"))
			offset, _ := strconv.Atoi(params.Get("offset"))

			if limit < 0 || limit > 25 {
				limit = 5
			}

			if offset < 0 {
				offset = 0
			}
		}

		//  GET /$table/$id
		if len(parts) == 2 {
			tableName := parts[0]
			id := parts[1]
			row := e.db.QueryRow("SELECT * FROM ? WHERE id=?", tableName, id)

			row.Scan(&id)
		}

	case "POST":
		// создаёт новую запись, данный по записи в теле запроса (POST-параметры)
		//POST /$table

		idx, _ := e.db.Exec(
			"INSERT INTO ? (? ? ? ? ?) 	VALUES (? ? ? ? ?)")
		lastId, _ := idx.LastInsertId()

	case "PUT":
		// PUT /$table/$id

	case "DELETE":

	}

}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {

	if db == nil {
		return nil, errors.New("db cannot be nil")
	}

	return &DbExplorer{
		db: db,
	}, nil
}

func (e *DbExplorer) getAllTableNames() ([]string, error) {
	q := `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'photolist';
`
	// row, _ := rows.Columns() // получает названия колонок
	rows, err := e.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	var resultTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		resultTables = append(resultTables, tableName)
	}

	return resultTables, rows.Err()
}
