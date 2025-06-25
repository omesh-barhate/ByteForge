package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/table"
	columnio "github.com/omesh-barhate/ByteForge/internal/table/column/io"
	"github.com/omesh-barhate/ByteForge/internal/table/wal"
)

const (
	BaseDir = "./data"
)

type Tables map[string]*table.Table

type Database struct {
	Name   string
	Path   string
	Tables Tables
}

func NewDatabase(name string) (*Database, error) {
	if !exists(name) {
		return nil, fmt.Errorf("NewDatabase: %w", NewDatabaseDoesNotExistError(name))
	}

	db := &Database{
		Name: name,
		Path: path(name),
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

func (db *Database) Close() error {
	var e error
	for _, t := range db.Tables {
		if err := t.Close(); err != nil {
			e = err
		}
	}
	return e
}

func CreateDatabase(name string) (*Database, error) {
	if exists(name) {
		return nil, fmt.Errorf("CreateDatabase: %w", NewDatabaseAlreadyExistsError(name))
	}

	if err := os.MkdirAll(path(name), 0777); err != nil {
		return nil, fmt.Errorf("CreateDatabase: %w", err)
	}

	return &Database{
		Name:   name,
		Path:   path(name),
		Tables: make(map[string]*table.Table),
	}, nil
}

func (db *Database) readTables() (Tables, error) {
	entries, err := os.ReadDir(db.Path)
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
		f, err := os.OpenFile(filepath.Join(db.Path, v.Name()), os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		parts := strings.Split(v.Name(), ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Database.readTables: %w", table.NewInvalidFilename(v.Name()))
		}
		idxFile, err := os.OpenFile(filepath.Join(db.Path, parts[0]+"_idx."+parts[1]), os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		fullTextIdxFile, err := os.OpenFile(filepath.Join(db.Path, parts[0]+"_fulltext_idx."+parts[1]), os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		r, err := io.NewReader(f)
		columnDefReader := columnio.NewColumnDefinitionReader(f, r)
		tableName, err := table.GetTableName(f)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		writeAheadLog, err := wal.NewWAL(db.Path, tableName)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		t, err := table.NewTable(f, idxFile, fullTextIdxFile, r, columnDefReader, writeAheadLog)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		if err = t.ReadColumnDefinitions(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		if err = t.SetRecordParser(parser.NewRecordParser(f, t.ColumnNames())); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		if err = t.LoadIdx(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		if err = t.LoadFullTextIdx(); err != nil {
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

func (db *Database) CreateTable(dbPath, name string, columnNames []string, columns table.Columns) (*table.Table, error) {
	path := filepath.Join(dbPath, name+table.FileExtension)
	idxPath := filepath.Join(dbPath, name+"_idx"+table.FileExtension)
	if _, err := os.Open(path); err == nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", NewTableAlreadyExistsError(name))
	}
	fullTextIdxPath := filepath.Join(dbPath, name+"_fulltext_idx"+table.FileExtension)
	if _, err := os.Open(path); err == nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", NewTableAlreadyExistsError(name))
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}
	idxFile, err := os.Create(idxPath)
	if err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}
	fullTextIdxFile, err := os.Create(fullTextIdxPath)
	if err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}

	r, err := io.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}
	columnDefReader := columnio.NewColumnDefinitionReader(f, r)
	recParser := parser.NewRecordParser(f, columnNames)
	writeAheadLog, err := wal.NewWAL(db.Path, name)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	t, err := table.NewTableWithColumns(f, idxFile, fullTextIdxFile, r, columnDefReader, writeAheadLog, columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}
	if err = t.SetRecordParser(recParser); err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
	}

	if err = t.WriteColumnDefinitions(); err != nil {
		return nil, fmt.Errorf("Database.CreateTable: %w", err)
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
