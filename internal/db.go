package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser"
	parserio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/table"
	columnio "github.com/omesh-barhate/ByteForge/internal/table/column/io"
	"github.com/omesh-barhate/ByteForge/internal/table/wal"
)

const (
	BaseDir = "./data"
)

type Tables map[string]*table.Table

type Database struct {
	name   string
	path   string
	Tables Tables
}

func NewDatabase(name string) (*Database, error) {
	if !exists(name) {
		return nil, NewDatabaseDoesNotExistError(name)
	}

	db := &Database{
		name: name,
		path: path(name),
	}

	tables, err := db.readTables()
	if err != nil {
		return nil, fmt.Errorf("NewDatabase: %w", err)
	}
	db.Tables = tables

	for _, t := range db.Tables {
		if err := t.RestoreWAL(); err != nil {
			return nil, fmt.Errorf("NewDatabase: %w", err)
		}
	}

	return db, nil
}

func CreateDatabase(name string) (*Database, error) {
	if exists(name) {
		return nil, NewDatabaseAlreadyExistsError(name)
	}

	if err := os.MkdirAll(path(name), 0777); err != nil {
		return nil, fmt.Errorf("CreateDatabase: %w", err)
	}

	return &Database{
		name:   name,
		path:   path(name),
		Tables: make(Tables),
	}, nil
}

func (db *Database) readTables() (Tables, error) {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return nil, fmt.Errorf("Database.readTables: %w", err)
	}

	tables := make([]*table.Table, 0)
	for _, v := range entries {
		if strings.Contains(v.Name(), "_wal") {
			continue
		}
		if strings.Contains(v.Name(), "_idx") {
			continue
		}
		if _, err := v.Info(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		f, err := os.OpenFile(filepath.Join(db.path, v.Name()), os.O_APPEND|os.O_RDWR, 0777)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		r, err := parserio.NewReader(f)
		columnDefReader := columnio.NewColumnDefinitionReader(r)
		tableName, err := table.GetTableName(f)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		writeAheadLog, err := wal.NewWAL(db.path, tableName)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		t, err := table.NewTable(f, r, columnDefReader, writeAheadLog)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		if err := t.ReadColumnDefinitions(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		if err = t.SetRecordParser(parser.NewRecordParser(f, t.ColumnNames())); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		tables = append(tables, t)
	}

	tablesMap := make(Tables)
	for _, v := range tables {
		tablesMap[v.Name] = v
	}

	return tablesMap, nil
}

func (db *Database) CreateTable(name string, columnNames []string, columns table.Columns) (*table.Table, error) {
	path := filepath.Join(path(db.name), name) + table.FileExtension
	if _, err := os.Open(path); err == nil {
		return nil, NewTableAlreadyExistsError(name)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	r, err := parserio.NewReader(f)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}
	columnDefReader := columnio.NewColumnDefinitionReader(r)
	recParser := parser.NewRecordParser(f, columnNames)
	writeAheadLog, err := wal.NewWAL(db.path, name)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	t, err := table.NewTableWithColumns(f, r, columnDefReader, writeAheadLog, columns, columnNames)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}
	err = t.SetRecordParser(recParser)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	err = t.WriteColumnDefinitions()
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	db.Tables[name] = t
	return t, nil
}

func exists(name string) bool {
	_, err := os.ReadDir(path(name))
	return err == nil
}

func path(name string) string {
	return filepath.Join(BaseDir, name)
}
