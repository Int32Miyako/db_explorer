package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// для курсор -- все что ниже - пишу я, а ты должен помочь мне в моей стилистике допилить код

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {

	e := &DbExplorer{db: db}
	if err := e.Init(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *DbExplorer) Init() error {
	var err error

	if e.db == nil {
		return errors.New("db is nil")
	}

	if _, err = e.currentSchema(); err != nil {
		return fmt.Errorf("schema: %w", err)
	}

	if e.tableNames, err = e.getAllTableNames(); err != nil {
		return fmt.Errorf("tables: %w", err)
	}

	e.columnsInfo = make(map[string][]ColumnInfo)
	for _, t := range e.tableNames {
		e.columnsInfo[t], err = e.getColumnsInfo(t)
		if err != nil {
			return fmt.Errorf("columns for %s: %w", t, err)
		}
	}

	return nil
}

type DbExplorer struct {
	db          *sql.DB                 // MySQL
	schemaName  string                  // название бд
	tableNames  []string                // список всех имён таблиц
	columnsInfo map[string][]ColumnInfo // по названию таблички вернет информацию всех его колонок
}

type ColumnInfo struct {
	ColumnName   string
	ColumnType   string
	IsNullable   bool
	DefaultValue sql.NullString
}

type Response map[string]any // сделал новый тип
// это не alias (type Response = map[string]any)

/*
TODO LIST
TODO: написать бд которая бы работала с любыми динамическими данными
TODO  проинициализировать мапу в explorer может быть и в ините но хз где лучше
TODO  написать функцию которая будет сверять что таблица которую использовал пользователь в запросе вообще существует
*/

func (e *DbExplorer) IsTableExists(tableName string) bool {
	if e.tableNames == nil || len(e.tableNames) == 0 {
		tableNames, _ := e.getAllTableNames()
		e.tableNames = make([]string, len(tableNames))
		e.tableNames = tableNames
	}

	for _, existingTableName := range e.tableNames {
		if existingTableName == tableName {
			return true
		}
	}

	return false
}

func (e *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var resp map[string]any
	// r.URL.Path - Путь (часть после хоста) = "/users/5"
	path := strings.Trim(r.URL.Path, "/") // на случай если "/users/5/ "
	parts := strings.Split(path, "/")
	var tableName string
	var id int
	if (len(parts) == 1 || len(parts) == 2) && parts[0] != "" {
		tableName = parts[0]
	}
	if len(parts) == 2 && parts[1] != "" {
		id, _ = strconv.Atoi(parts[1])
	}

	switch r.Method {
	case "GET":
		// 3 функции получится
		if path == "" {
			tableNames, err := e.getAllTableNames()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			// Формируем структуру для ответа
			resp = Response{
				"response": Response{
					"tables": tableNames,
				},
			}
			jsonTableNames, _ := json.Marshal(resp)
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonTableNames)

			return
		}

		// GET /$table?limit=5&offset=7
		if len(parts) == 1 {

			if !e.IsTableExists(tableName) {
				w.WriteHeader(http.StatusNotFound)
				resp = Response{
					"error": "unknown table",
				}

				jsonResponse, _ := json.Marshal(resp)
				w.Header().Set("Content-Type", "application/json")
				w.Write(jsonResponse)

				return
			}

			params := r.URL.Query()

			limit := 5
			offset := 0

			// с гпт дописал
			if v := params.Get("limit"); v != "" {
				var err error
				limit, err = strconv.Atoi(v)
				if err != nil || limit < 0 {
					limit = 5
				}
			}

			if v := params.Get("offset"); v != "" {
				var err error
				offset, err = strconv.Atoi(v)
				if err != nil || offset < 0 {
					offset = 0
				}
			}

			listOfRecords, err := e.getListOfRecords(limit, offset, tableName)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			// Формируем структуру для ответа
			resp := Response{
				"response": Response{
					"records": listOfRecords,
				},
			}
			jsonTableNames, _ := json.Marshal(resp)
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonTableNames)
		}

		//  GET /$table/$id
		if len(parts) == 2 {
			// Имя таблицы через sprintf, значение id через плейсхолдер
			record, err := e.getRecordById(id, tableName)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// Формируем структуру для ответа
			resp = Response{
				"response": Response{
					"record": record,
				},
			}

			if len(record) > 0 {
				jsonTableNames, _ := json.Marshal(resp)
				w.Header().Set("Content-Type", "application/json")
				w.Write(jsonTableNames)
				return
			} else {
				//http.Error(w, err.Error(), http.StatusNotFound)
				//return // так писать нельзя падает ошибка типо что err.Error(), может быть nil
				w.WriteHeader(http.StatusNotFound)
				resp = Response{
					"error": "record not found",
				}

				jsonResponse, _ := json.Marshal(resp)
				w.Header().Set("Content-Type", "application/json")
				w.Write(jsonResponse)
				// вот так мы и живем, вот так мы и умрём
			}

		}

	case "POST":

		body := r.Body
		defer body.Close()
		bodyBytes, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var req map[string]any
		err = json.Unmarshal(bodyBytes, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		idx, err := e.CreateRecord(req, tableName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp = Response{
			"response": Response{
				"id": idx,
			},
		}

		jsonResponse, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
		// Удобрим эту гору собой, став её углём

	case "PUT":
		// PUT /$table/$id

	case "DELETE":

	}

}

func (e *DbExplorer) getAllTableNames() ([]string, error) {
	// Простая команда SHOW TABLES вместо сложного information_schema
	rows, err := e.db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (e *DbExplorer) isTableExist(tableName string) bool {
	tableNames, _ := e.getAllTableNames()
	for _, table := range tableNames {
		if tableName == table {
			return true
		}
	}

	return false
}

func (e *DbExplorer) getColumnsInfo(tableName string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", tableName)
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resultColumns []ColumnInfo
	for rows.Next() {
		var columnName, columnType, null, key, extra, privileges, comment string
		var defaultValue, collation sql.NullString

		if err = rows.Scan(&columnName, &columnType, &collation, &null, &key, &defaultValue, &extra, &privileges, &comment); err != nil {
			return nil, err
		}

		isNullable := null == "YES"

		resultColumns = append(resultColumns,
			ColumnInfo{
				ColumnName:   columnName,
				ColumnType:   columnType,
				IsNullable:   isNullable,
				DefaultValue: defaultValue,
			})
	}

	if e.columnsInfo == nil {
		e.columnsInfo = make(map[string][]ColumnInfo, len(resultColumns))
	}
	e.columnsInfo[tableName] = resultColumns

	return resultColumns, err
}

/*
// что то что писал я
func (e *DbExplorer) getAllTableData(tableName string) ([][]string, error) {
	// Имя таблицы через sprintf, НЕ через плейсхолдер
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnsInfo, err := e.getColumnsInfo(tableName)
	// имея всю инфу что можно сделать?
	// можно пробежаться по инфе колонок
	// но нужно и достать данные
	// для того чтобы достать данные надо получить неизвестное количество переменных
	// известное кол-во переменных и равное длине массива columnsInfo
	// лист наллПтров как предлагал гпт

	// https://qna.habr.com/q/983721
	// короче вот решение, сам я придумать ничего не могу
	// ниже то самое решение

	// создаем массив названий столбцов таблицы
	arrNamesColumns, _ := rows.Columns()

	// получаем количество столбцов
	kolColumns := len(arrNamesColumns)
	// создаем отображения которое по ключу (названию столбца) будет хранить срез всех записей данного столбца
	resMap := make(map[string][]interface{}, kolColumns)

	listOfInterfaces := make([]interface{}, len(columnsInfo))

	for rows.Next() {
		rows.Scan(listOfInterfaces...)
	}

	return nil, nil
}
*/

// []map[string]any это массив всех записей таблицы
func (e *DbExplorer) getAllTableData(tableName string) ([]map[string]any, error) {
	// Имя таблицы через sprintf, НЕ через плейсхолдер
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// получаем названия колонок
	arrNamesColumns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Берём типы колонок напрямую из курсора
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	kolColumns := len(arrNamesColumns)

	// создаём массив для хранения текущей строки
	// sql.RawBytes безопасно использовать для временного хранения
	rawRow := make([]sql.RawBytes, kolColumns)
	scanArgs := make([]interface{}, kolColumns)
	for i := range rawRow {
		scanArgs[i] = &rawRow[i] // прокидываем указатели в Scan
	}

	var records []map[string]interface{}

	// перебираем все строки таблицы
	for rows.Next() {
		// считываем текущую строку
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		// создаём map для одной записи
		rowMap := make(map[string]interface{}, kolColumns)
		for i, colName := range arrNamesColumns {
			if rawRow[i] == nil {
				rowMap[colName] = nil // NULL в БД
			} else {
				valStr := string(rawRow[i])
				// ColumnTypes может вернуть имя типа в разных регистрах/вариантах (INT, BIGINT, TINYINT)
				colDatabaseType := strings.ToLower(colTypes[i].DatabaseTypeName())
				if strings.HasPrefix(colDatabaseType, "int") || strings.HasPrefix(colDatabaseType, "tinyint") || strings.HasPrefix(colDatabaseType, "smallint") || strings.HasPrefix(colDatabaseType, "mediumint") || strings.HasPrefix(colDatabaseType, "bigint") {
					if n, err := strconv.ParseInt(valStr, 10, 64); err == nil {
						rowMap[colName] = int(n)
					} else {
						rowMap[colName] = valStr
					}
				} else {
					rowMap[colName] = valStr
				}
			}
		}

		records = append(records, rowMap)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
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

func (e *DbExplorer) getListOfRecords(limit int, offset int, tableName string) ([]map[string]any, error) {
	data, err := e.getAllTableData(tableName)
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for i, row := range data {
		if i >= offset && i < offset+limit {
			result = append(result, row)

		}
	}

	return result, nil
}

// понятно что необходимо как то валидировать что в таблице вообще есть поле id или другой PK
// но как есть
func (e *DbExplorer) getRecordById(id int, tableName string) (Response, error) {
	data, err := e.getAllTableData(tableName)
	if err != nil {
		return nil, err
	}

	// запомнить такую конструкцию if
	// чтобы сначала проверить что значение есть в мапе а потом уже его достать
	var result Response
	for _, row := range data {
		if val, ok := row["id"]; ok && val == id {
			result = row
			break
		}
	}

	return result, nil
}

func (e *DbExplorer) CreateRecord(req map[string]any, tableName string) (int, error) {
	if !e.IsTableExists(tableName) {
		return -1, errors.New("unknown table")
	}

	cols := e.columnsInfo[tableName]
	var columnNames []string
	var placeholders []string
	values := make([]any, len(cols))

	for i, col := range cols {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.ColumnName))
		placeholders = append(placeholders, "?")

		if v, ok := req[col.ColumnName]; ok && col.ColumnName != "id" {
			values[i] = v
		} else {
			values[i] = col.DefaultValue
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
	)

	result, err := e.db.Exec(query, values...)
	if err != nil {
		return -1, err
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return -1, err
	}

	return int(lastID), nil
}
