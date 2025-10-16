package main

import (
	"database/sql"
	"encoding/json"
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

type ColumnInfo struct {
	ColumnName   string
	ColumnType   string
	IsNullable   bool
	DefaultValue sql.NullString
}

type Response map[string]any

// GetNumberOfQuestions
// чтобы добавить ровно столько ? сколько нужно вставить в запрос
// можно попробовать проитерироваться GetNumberOfQuestion() раз и
// вставить ровно столько значений в запрос
func (e *DbExplorer) GetNumberOfQuestions() int {
	return len(e.columnNames)
}

/*
TODO LIST
TODO: написать бд которая бы работала с любыми динамическими данными
TODO  проинициализировать мапу в explorer может быть и в ините но хз где лучше
TODO  написать функцию которая будет сверять что таблица которую использовал пользователь в запросе вообще существует
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
	var resp map[string]any
	// r.URL.Path - Путь (часть после хоста) = "/users/5"
	path := strings.Trim(r.URL.Path, "/") // на случай если "/users/5/ "
	parts := strings.Split(path, "/")
	var tableName string
	var id int
	if len(parts) == 1 && parts[0] != "" {
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
			query := fmt.Sprintf("SELECT * FROM `%s` WHERE id=?", tableName)
			row := e.db.QueryRow(query, id)

			// TODO: Здесь нужно правильно обработать результат
			row.Scan(&id)
		}

	case "POST":
		_, err := e.createTable(tableName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
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
		var columnName, columnType, collation, null, key, extra, privileges, comment string
		var defaultValue sql.NullString

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

	// получаем мета-информацию по колонкам, чтобы знать их типы
	columnsInfo, err := e.getColumnsInfo(tableName)
	if err != nil {
		return nil, err
	}
	// мапим имя колонки -> тип (как строку из MySQL, например: "int(11)")
	columnNameToType := make(map[string]string, len(columnsInfo))
	for _, c := range columnsInfo {
		columnNameToType[c.ColumnName] = c.ColumnType
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
				colType := columnNameToType[colName]

				// если тип колонки целочисленный — конвертируем значение в int
				if strings.HasPrefix(colType, "int") || strings.HasPrefix(colType, "tinyint") || strings.HasPrefix(colType, "smallint") || strings.HasPrefix(colType, "mediumint") || strings.HasPrefix(colType, "bigint") {
					if n, err := strconv.ParseInt(valStr, 10, 64); err == nil {
						rowMap[colName] = int(n)
					} else {
						// если по какой-то причине не распарсили — отдадим как строку
						rowMap[colName] = valStr
					}
				} else {
					rowMap[colName] = valStr // для остальных типов оставляем строку
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

// создаёт новую запись, данный по записи в теле запроса (POST-параметры)
// POST /$table
func (e *DbExplorer) createTable(tableName string) (int, error) {

	if !e.IsTableExists(tableName) {
		return 0, errors.New("unknown table")
	}

	columnNamesStr := `(`
	stringsQuestions := `(`
	for i := 0; i < len(e.columnNames); i++ {
		columnNamesStr += " " + fmt.Sprintf("%s", e.columnNames[tableName][i])
		stringsQuestions += ` ?,`
	}
	stringsQuestions += ")"
	columnNamesStr += ")"

	// Создаем строку с плейсхолдерами для значений
	placeholdersStr := "("
	for i := 0; i < len(e.columnNames); i++ {
		if i > 0 {
			placeholdersStr += ","
		}
		placeholdersStr += "?"
	}
	placeholdersStr += ")"

	for i := 0; i < len(e.columnNames); i++ {

	}

	// Собираем INSERT запрос через sprintf для имен таблиц/колонок
	insertQuery := fmt.Sprintf("INSERT INTO `%s` %s VALUES %s", tableName, columnNamesStr, placeholdersStr)

	// TODO: Здесь нужно получить данные из тела запроса и передать их как значения
	// Пока что создаем пустые значения для демонстрации
	values := make([]interface{}, len(e.columnNames))
	for i := range values {
		values[i] = nil // или дефолтные значения
	}

	idx, err := e.db.Exec(insertQuery, values...)
	if err != nil {
		return 0, err
	}

	lastId, _ := idx.LastInsertId()
	fmt.Print(strconv.Itoa(int(lastId)))

	return int(lastId), nil
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
