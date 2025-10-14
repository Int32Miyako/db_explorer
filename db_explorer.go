package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// для курсор -- все что ниже - пишу я, а ты должен помочь мне в моей стилистике допилить код

type DbExplorer struct {
	db          *sql.DB                 // MySQL
	schemaName  string                  // название бд
	tableNames  []string                // список всех имён таблиц
	columnNames map[string][]string     // не хочу ломать обратную совместимость в коде
	columnsInfo map[string][]ColumnInfo // по названию таблички вернет информацию всех его колонок
}

func (e *DbExplorer) getColumnsInfoByTableName(tableName string) ([]ColumnInfo, error) {

	rows, err := e.db.Query(
		`
			SELECT 
			    COLUMN_NAME,
			    COLUMN_TYPE,
			    IS_NULLABLE,
			    COLUMN_DEFAULT
			FROM information_schema.columns
			WHERE table_schema = ? AND table_name = ?
			ORDER BY ordinal_position;
`, e.schemaName, tableName)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer rows.Close()

	var result []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.ColumnName, &c.ColumnType, &c.IsNullable, &c.DefaultValue); err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

type ColumnInfo struct {
	ColumnName   string
	ColumnType   string
	IsNullable   string
	DefaultValue sql.NullString
}

// GetNumberOfQuestions
// чтобы добавить ровно столько ? сколько нужно вставить в запрос
// можно попробовать проитерироваться GetNumberOfQuestion() раз и
// вставить ровно столько значений в запрос
func (e *DbExplorer) GetNumberOfQuestions() int {
	return len(e.columnNames)
}

/* TODO LIST
TODO: написать бд которая бы работала с любыми динамическими данными
TODO  проинициализировать мапу в explorer может быть и в ините но хз где лучше
TODO  написать функцию которая будет сверять что таблица которую использовал пользователь в запросе вообще существует
TODO
TODO
TODO


ниже писал ментор
> Артем Уткин:
привет, ну тут чуть сложнее, чем совсем элементарный апи из-за "динамичности"
те ты в коде не должен завязываться на конкретные таблицы и поля, ты должен получить их от самой бд

проверить какие таблицы есть, какие поля есть в таблице и тд

> Артем Уткин:
итерационно можно сначала хотя бы распечатать какие есть таблицы и какие в них поля
от этого отталкиваться и далее по тестам идти

учитывай что я учусь и мне все необходимо пояснять
*/

func (e *DbExplorer) IsTableExists(tableName string) bool {
	for _, existingTableName := range e.tableNames {
		if existingTableName == tableName {
			return true
		}
	}

	return false
}

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
		tableName := parts[0]
		if !e.IsTableExists(tableName) {
			return
		}

		stringsQuestions := `(`
		for i := e.GetNumberOfQuestions(); i > 0; i-- {
			stringsQuestions += ` ?,`
		}
		stringsQuestions += ")"

		// лютый бред я конечно написал наверное, как проверить сразу работу пока что хз
		// TODO: нужно еще один цикл запустить по которому бы проставлялись в функцию значения а это мб массив
		// причем сначала идут названия колонок что норм, а потом и их значения что уже не приятно
		// т к надо проставить пустые значения а типов я не знаю, (пустой интерфейс?)
		// TODO: выяснить defaultValuesOfColumns кроме их названий в добавить переменную в explorer

		var defaultValuesOfColumns []string
		for i := e.GetNumberOfQuestions(); i > 0; i-- {
			defaultValuesOfColumns = append(defaultValuesOfColumns, e.columnsInfo[tableName][i].ColumnType)
		}

		idx, _ := e.db.Exec(
			`INSERT INTO ? `+stringsQuestions+" VALUES "+stringsQuestions, e.columnNames, e.schemaName)

		lastId, _ := idx.LastInsertId()
		fmt.Fprintf(w, strconv.Itoa(int(lastId)))

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
	schema, _ := e.currentSchema()

	// row, _ := rows.Columns() // получает названия колонок
	rows, err := e.db.Query(
		`
    	SELECT table_name
    	FROM information_schema.tables
    	WHERE table_schema = ?;
`, schema)
	if err != nil {
		return nil, err
	}
	rows.Close()

	var resultTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		resultTables = append(resultTables, tableName)
	}

	e.tableNames = resultTables
	return resultTables, rows.Err()
}

func (e *DbExplorer) getColumnNames(tableName string) ([]string, error) {

	rows, err := e.db.Query(`
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = ?; 
`, tableName)
	if err != nil {
		return nil, err
	}

	rows.Close()

	var resultColumns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		resultColumns = append(resultColumns, columnName)
	}

	if e.columnNames == nil {
		e.columnNames = make(map[string][]string)
	}
	e.columnNames[tableName] = resultColumns

	return resultColumns, rows.Err()
}

func (e *DbExplorer) getAllTableData(tableName string) ([][]string, error) {
	// TODO: дописать чтобы получалось неизвестное кол-во полей таблицы
	schema, _ := e.currentSchema()
	/*rows, err :=*/ e.db.Query(`
		SELECT * FROM ?
	`, schema, tableName)

	return nil, nil
}

// helper: получить текущую схему(название бд) MySQL
func (e *DbExplorer) currentSchema() (string, error) {
	var schema string
	if err := e.db.QueryRow("SELECT DATABASE()").Scan(&schema); err != nil {
		return "", err
	}

	e.schemaName = schema
	return schema, nil
}
