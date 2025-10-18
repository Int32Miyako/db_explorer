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
// для всех агентов -- все что ниже - пишу я, а ты должен помочь мне в моей стилистике допилить код
// не надо добавлять првоерку на Primary Key
// по заданию для этого точно будет использоваться id
// так что лишнюю сложность не надо

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
	if len(e.tableNames) == 0 {
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
	var req, resp map[string]any
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
	case http.MethodGet:
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

	case http.MethodPost:

		body := r.Body
		defer body.Close()
		bodyBytes, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = json.Unmarshal(bodyBytes, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqValid := e.ValidRequest(req, tableName)
		idx, err := e.CreateRecord(reqValid, tableName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		primaryKey := e.GetPrimaryKey(tableName)
		resp = Response{
			"response": Response{
				primaryKey: idx,
			},
		}

		jsonResponse, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)

		// Удобрим эту гору собой, став её углём

	case http.MethodPut:
		body := r.Body
		defer body.Close()
		bodyBytes, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = json.Unmarshal(bodyBytes, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		idx, err := e.UpdateRecord(req, tableName, id)

		if err != nil {
			WriteError(w, err)
			return
		}

		resp = Response{
			"response": Response{
				"updated": idx,
			},
		}

		jsonResponse, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)

	case http.MethodDelete:
		deleted, err := e.DeleteRecord(tableName, id)
		if err != nil {
			WriteError(w, err)
			return
		}

		resp = Response{
			"response": Response{
				"deleted": deleted,
			},
		}

		jsonResponse, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)

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
				if strings.Contains(colDatabaseType, "int") {
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

	// получение ключевого поля таблицы, предпологая что оно идет первым без выискивания PK
	primaryKey := e.columnsInfo[tableName][0].ColumnName

	// запомнить такую конструкцию if
	// чтобы сначала проверить что значение есть в мапе а потом уже его достать
	var result Response
	for _, row := range data {
		if val, ok := row[primaryKey]; ok && val == id {
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

		if v, ok := req[col.ColumnName]; ok && col.ColumnName != e.GetPrimaryKey(tableName) {
			values[i] = v
		} else {
			// Используем дефолтное значение
			if col.DefaultValue.Valid {
				values[i] = col.DefaultValue.String
			} else {
				// Если дефолтного значения нет
				if col.IsNullable {
					values[i] = nil
				} else {
					// Для NOT NULL полей используем пустое значение по типу
					if strings.Contains(col.ColumnType, "varchar") || strings.Contains(col.ColumnType, "text") {
						values[i] = ""
					} else if strings.HasPrefix(col.ColumnType, "int") {
						values[i] = 0
					} else {
						values[i] = nil
					}
				}
			}
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

func (e *DbExplorer) UpdateRecord(req map[string]any, tableName string, id int) (int, error) {
	if !e.IsTableExists(tableName) {
		return -1, errors.New("unknown table")
	}

	cols := e.columnsInfo[tableName]
	var sets []string
	var values []any

	for _, col := range cols {
		// Проверяем, что primary key не пытаются обновить
		if _, ok := req[col.ColumnName]; ok && col.ColumnName == e.GetPrimaryKey(tableName) {
			return -2, fmt.Errorf("field %s have invalid type", e.GetPrimaryKey(tableName))
		}

		// ok — булево, которое показывает, есть ли вообще такой ключ в мапе
		if _, ok := req[col.ColumnName]; ok {
			err := e.validateFieldType(col, req[col.ColumnName], true)
			if err != nil {
				return -2, err
			}

		}

		// Пропускаем primary key при обновлении
		if strings.Contains(col.ColumnName, e.GetPrimaryKey(tableName)) {
			continue
		}

		// Если поле есть в запросе - валидируем и добавляем
		if v, ok := req[col.ColumnName]; ok {
			sets = append(sets, fmt.Sprintf("`%s` = ?", col.ColumnName))
			values = append(values, v)
		}
	}

	query := fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE `%s` = ?",
		tableName,
		strings.Join(sets, ", "),
		e.GetPrimaryKey(tableName),
	)

	values = append(values, id)
	result, err := e.db.Exec(query, values...)
	if err != nil {
		return -1, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return -1, err
	}
	return int(rowsAffected), nil
}

// DeleteRecord DELETE /$table/$id - удаляет запись
func (e *DbExplorer) DeleteRecord(tableName string, id int) (int, error) {
	if !e.IsTableExists(tableName) {
		return 0, errors.New("unknown table")
	}

	query := fmt.Sprintf("DELETE FROM `%s` WHERE `id` = ?", tableName)
	result, err := e.db.Exec(query, id)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(rowsAffected), nil
}

func WriteError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	code := http.StatusInternalServerError
	msg := err.Error()

	// Все ошибки вида "field X have invalid type" должны возвращать 400
	if strings.Contains(msg, "field ") && strings.Contains(msg, " have invalid type") {
		code = http.StatusBadRequest
	}

	w.WriteHeader(code)
	resp := Response{"error": msg}
	jsonResp, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResp)
}

// эту функцию написал изначально курсор, я решил что оно не нужно
// но походу тесты прям намекают валидацию поля вынести
// value это значение которое мы валидируем (nil string int)
func (e *DbExplorer) validateFieldType(col ColumnInfo, value interface{}, isUpdate bool) error {
	err := fmt.Errorf("field %s have invalid type", col.ColumnName)

	// Если значение nil
	if value == nil {
		// Для nullable полей nil - это валидное значение
		if col.IsNullable {
			return nil
		}
		// Для NOT NULL полей nil - это ошибка
		return err
	}

	// Проверяем тип значения
	switch {
	case strings.HasPrefix(col.ColumnType, "int"):
		if _, ok := value.(int); !ok {
			// В JSON числа могут приходить как float64
			if _, ok := value.(float64); !ok {
				return err
			}
		}

	case strings.Contains(col.ColumnType, "varchar") || strings.Contains(col.ColumnType, "text"):
		if _, ok := value.(string); !ok {
			return err
		}

	}

	return nil
}

func (e *DbExplorer) GetPrimaryKey(tableName string) string {

	return e.columnsInfo[tableName][0].ColumnName
}

// ValidRequest принимает в себя запрос и отдает обработанный от всех ошибок которые
// могли возникнуть, например неправильное название поля будет игнорироваться
func (e *DbExplorer) ValidRequest(req map[string]any, tableName string) map[string]any {
	validReq := make(map[string]any)

	for _, col := range e.columnsInfo[tableName] {
		if val, ok := req[col.ColumnName]; ok {
			// если в запросе есть такое поле — сохраняем
			validReq[col.ColumnName] = val
		}
	}

	return validReq
}
