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

const (
	StatusInternalServerError = -1
	//badRequestError           = -2
	//dbError                   = -3
)

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
	var tableName string
	var id int

	// r.URL.Path - Путь (часть после хоста) = "/users/5"
	// Trim на случай если "/users/5/ "
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if (len(parts) == 1 || len(parts) == 2) && parts[0] != "" {
		tableName = parts[0]
	}
	if len(parts) == 2 && parts[1] != "" {
		id, _ = strconv.Atoi(parts[1])
	}

	switch r.Method {
	case http.MethodGet:
		// GET /
		if r.URL.Path == "/" {
			GetTables(e, w)
		}

		// GET /$table?limit=5&offset=7
		if len(parts) == 1 && tableName != "" {
			GetRecords(e, w, r, tableName)
		}

		//  GET /$table/$id
		if len(parts) == 2 && tableName != "" && id > 0 {
			GetRecord(e, w, tableName, id)
		}

	case http.MethodPost:
		CreateRecord(e, w, r, tableName) // POST /$table

	case http.MethodPut:
		UpdateRecord(e, w, r, tableName, id) // PUT /$table/$id

	case http.MethodDelete:
		DeleteRecords(e, w, tableName, id) // DELETE /$table/$id
	}

}

func (e *DbExplorer) getAllTableNames() ([]string, error) {
	// Простая команда SHOW TABLES вместо сложного information_schema
	rows, err := e.db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {

		}
	}(rows)

	var resultTables []string
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
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
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

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

// []map[string]any это массив всех записей таблицы
func (e *DbExplorer) getAllTableData(tableName string) ([]map[string]any, error) {
	// Имя таблицы через sprintf, НЕ через плейсхолдер
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {

		}
	}(rows)

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

	var records []map[string]any

	// перебираем все строки таблицы
	for rows.Next() {
		// считываем текущую строку
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		// создаём map для одной записи
		rowMap := make(map[string]any, kolColumns)
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

func (e *DbExplorer) GetRecordById(id int, tableName string) (Response, error) {
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
		return StatusInternalServerError, errors.New("unknown table")
	}

	cols := e.columnsInfo[tableName]
	var columnNames []string
	var placeholders []string
	values := make([]any, len(cols))

	for i, col := range cols {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.ColumnName))
		placeholders = append(placeholders, "?")

		if v, ok := req[col.ColumnName]; ok && col.ColumnName != e.getPrimaryKey(tableName) {
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
		return StatusInternalServerError, err
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return StatusInternalServerError, err
	}

	return int(lastID), nil
}

func (e *DbExplorer) UpdateRecord(req map[string]any, tableName string, id int) (int, error) {
	if !e.IsTableExists(tableName) {
		return StatusInternalServerError, errors.New("unknown table")
	}

	cols := e.columnsInfo[tableName]
	var sets []string
	var values []any

	for _, col := range cols {
		// Проверяем, что primary key не пытаются обновить
		if _, ok := req[col.ColumnName]; ok && col.ColumnName == e.getPrimaryKey(tableName) {
			return StatusInternalServerError, fmt.Errorf("field %s have invalid type", e.getPrimaryKey(tableName))
		}

		// ok — булево, которое показывает, есть ли вообще такой ключ в мапе
		if _, ok := req[col.ColumnName]; ok {
			err := validateFieldType(col, req[col.ColumnName])
			if err != nil {
				return StatusInternalServerError, err
			}

		}

		// Пропускаем primary key при обновлении
		if strings.Contains(col.ColumnName, e.getPrimaryKey(tableName)) {
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
		e.getPrimaryKey(tableName),
	)

	values = append(values, id)
	result, err := e.db.Exec(query, values...)
	if err != nil {
		return StatusInternalServerError, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return StatusInternalServerError, err
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

	writeJsonResponse(w, resp)
}

// эту функцию написал изначально курсор, я решил что оно не нужно
// но походу тесты прям намекают валидацию поля вынести
// value это значение которое мы валидируем (nil string int)
func validateFieldType(col ColumnInfo, value interface{}) error {
	err := fmt.Errorf("field %s have invalid type", col.ColumnName)

	if value == nil {
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
			if _, ok = value.(float64); !ok {
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

func (e *DbExplorer) getPrimaryKey(tableName string) string {
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

func GetTables(e *DbExplorer, w http.ResponseWriter) {
	tableNames, err := e.getAllTableNames()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	resp := Response{
		"response": Response{
			"tables": tableNames,
		},
	}
	jsonTableNames, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonTableNames)
	if err != nil {
		return
	}
}

func GetRecords(e *DbExplorer, w http.ResponseWriter, r *http.Request, tableName string) {

	if !e.IsTableExists(tableName) {
		w.WriteHeader(http.StatusNotFound)
		resp := Response{
			"error": "unknown table",
		}

		writeJsonResponse(w, resp)

		return
	}

	params := r.URL.Query()

	limit := 5
	offset := 0

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
	writeJsonResponse(w, resp)

}

func GetRecord(e *DbExplorer, w http.ResponseWriter, tableName string, id int) {
	// Имя таблицы через sprintf, значение id через плейсхолдер
	record, err := e.GetRecordById(id, tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := Response{
		"response": Response{
			"record": record,
		},
	}

	if len(record) > 0 {
		writeJsonResponse(w, resp)
	} else {
		w.WriteHeader(http.StatusNotFound)
		resp = Response{
			"error": "record not found",
		}

		writeJsonResponse(w, resp)
	}

}

func CreateRecord(e *DbExplorer, w http.ResponseWriter, r *http.Request, tableName string) {
	var req map[string]any

	body := r.Body
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {

		}
	}(body)
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		WriteError(w, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		WriteError(w, err)
		return
	}

	reqValid := e.ValidRequest(req, tableName)
	idx, err := e.CreateRecord(reqValid, tableName)
	if err != nil {
		WriteError(w, err)
		return
	}

	primaryKey := e.getPrimaryKey(tableName)
	resp := Response{
		"response": Response{
			primaryKey: idx,
		},
	}

	writeJsonResponse(w, resp)
}

func UpdateRecord(e *DbExplorer, w http.ResponseWriter, r *http.Request, tableName string, id int) {
	var req map[string]any

	body := r.Body
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {

		}
	}(body)
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		WriteError(w, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		WriteError(w, err)
		return
	}

	idx, err := e.UpdateRecord(req, tableName, id)

	if err != nil {
		WriteError(w, err)
		return
	}

	resp := Response{
		"response": Response{
			"updated": idx,
		},
	}

	writeJsonResponse(w, resp)

}

func DeleteRecords(e *DbExplorer, w http.ResponseWriter, tableName string, id int) {
	deleted, err := e.DeleteRecord(tableName, id)
	if err != nil {
		WriteError(w, err)
		return
	}

	resp := Response{
		"response": Response{
			"deleted": deleted,
		},
	}

	writeJsonResponse(w, resp)
}

func writeJsonResponse(w http.ResponseWriter, resp Response) {
	jsonResponse, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(jsonResponse)
	if err != nil {
		return
	}
}
