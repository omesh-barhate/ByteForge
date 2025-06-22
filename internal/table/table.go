package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	platformio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
	"github.com/omesh-barhate/ByteForge/internal/table/column"
	columnio "github.com/omesh-barhate/ByteForge/internal/table/column/io"
	"github.com/omesh-barhate/ByteForge/internal/table/wal"
	walencoding "github.com/omesh-barhate/ByteForge/internal/table/wal/encoding"
)

const (
	FileExtension = ".bin"
)

type (
	Columns map[string]*column.Column

	Table struct {
		Name        string
		file        *os.File
		columnNames []string
		columns     Columns

		reader          *platformio.Reader
		recordParser    *parser.RecordParser
		columnDefReader *columnio.ColumnDefinitionReader
		wal             *wal.WAL
	}

	DeletableRecord struct {
		offset int64
		len    uint32
	}
)

func NewTable(
	f *os.File,
	reader *platformio.Reader,
	columnDefReader *columnio.ColumnDefinitionReader,
	wal *wal.WAL,
) (*Table, error) {
	if f == nil || reader == nil || columnDefReader == nil {
		return nil, fmt.Errorf("NewTable: nil argument")
	}
	tableName, err := GetTableName(f)
	if err != nil {
		return nil, fmt.Errorf("NewTable: %w", err)
	}
	t := &Table{
		file:            f,
		Name:            tableName,
		columns:         make(Columns),
		reader:          reader,
		columnDefReader: columnDefReader,
		wal:             wal,
	}
	return t, nil
}

func NewTableWithColumns(
	f *os.File,
	reader *platformio.Reader,
	columnDefReader *columnio.ColumnDefinitionReader,
	wal *wal.WAL,
	columns Columns,
	columnNames []string,
) (*Table, error) {
	t, err := NewTable(f, reader, columnDefReader, wal)
	if err != nil {
		return nil, fmt.Errorf("NewTableWithColumns: %w", err)
	}
	if err = t.SetColumns(columns, columnNames); err != nil {
		return nil, fmt.Errorf("NewTableWithColumns: %w", err)
	}
	return t, nil
}

func newDeletableRecord(offset int64, len uint32) *DeletableRecord {
	return &DeletableRecord{
		offset: offset,
		len:    len,
	}
}

func (t *Table) SetColumns(columns Columns, columnNames []string) error {
	if len(columnNames) == 0 {
		return fmt.Errorf("Table.SetColumns: columns cannot be empty")
	}
	if len(columns) == 0 {
		return fmt.Errorf("Table.SetColumns: columns cannot be empty")
	}
	t.columns = columns
	t.columnNames = columnNames
	return nil
}

func (t *Table) ColumnNames() []string {
	return t.columnNames
}

func (t *Table) SetRecordParser(recParser *parser.RecordParser) error {
	if recParser == nil {
		return fmt.Errorf("Table.SetRecordParser: recParser cannot be nil")
	}
	t.recordParser = recParser
	return nil
}

func (t *Table) WriteColumnDefinitions() error {
	for _, c := range t.columnNames {
		b, err := t.columns[c].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}

		colWriter := columnio.NewColumnDefinitionWriter(t.file)
		if _, err = colWriter.Write(b); err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
	}
	return nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
	}

	for {
		buf := make([]byte, 1024)
		n, err := t.columnDefReader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		col := column.Column{}
		if err = col.UnmarshalBinary(buf[:n]); err != nil {
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		colName := col.NameToStr()
		t.columns[colName] = &col
		t.columnNames = append(t.columnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]interface{}, useWAL bool) (int, error) {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if err := t.validateColumns(record); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	var sizeOfRecord uint32 = 0
	for _, col := range t.columnNames {
		val, ok := record[col]
		if !ok {
			return 0, fmt.Errorf("Table.Insert: missing column: %s", col)
		}
		tlvMarshaler := encoding.NewTLVMarshaler(val)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}

	byteMarshaler := encoding.NewValueMarshaler(types.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(typeBuf)

	intMarshaler := encoding.NewValueMarshaler(sizeOfRecord)
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(lenBuf)

	for _, col := range t.columnNames {
		v := record[col]
		tlvMarshaler := encoding.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(b)
	}

	var walEntry *wal.Entry
	if useWAL {
		walEntry, err = t.wal.Append(walencoding.OpInsert, t.Name, buf.Bytes())
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if n != buf.Len() {
		return 0, columnio.NewIncompleteWriteError(n, buf.Len())
	}

	if useWAL {
		if err := t.wal.Commit(walEntry); err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
	}

	return 1, nil
}

func (t *Table) seekUntil(targetType byte) error {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("Table.seekUntil: readByte: %w", err)
		}
		if dataType == targetType {
			if _, err = t.file.Seek(-1*types.LenByte, io.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: %w", err)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
		}

		if _, err = t.file.Seek(int64(length), io.SeekCurrent); err != nil {
			return fmt.Errorf("Table.seekUntil: %w", err)
		}
	}
}

func (t *Table) Select(whereStmt map[string]interface{}) ([]map[string]interface{}, error) {
	if err := t.ensureFilePointer(); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}
	if err := t.validateWhereStmt(whereStmt); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}

	results := make([]map[string]interface{}, 0)
	for {
		err := t.recordParser.Parse()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		rawRecord := t.recordParser.Value

		if err = t.ensureColumnLength(rawRecord.Record); err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		if !t.evaluateWhereStmt(whereStmt, rawRecord.Record) {
			continue
		}
		results = append(results, rawRecord.Record)
	}
}

func (t *Table) Update(whereStmt map[string]interface{}, values map[string]interface{}) (int, error) {
	if err := t.validateColumns(values); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	deletableRecords := make([]*DeletableRecord, 0)
	rawRecords := make([]*parser.RawRecord, 0)
	for {
		err := t.recordParser.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
		rawRecord := t.recordParser.Value

		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}

		if !t.evaluateWhereStmt(whereStmt, rawRecord.Record) {
			continue
		}

		rawRecords = append(rawRecords, rawRecord)
		pos, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
		deletableRecords = append(deletableRecords, newDeletableRecord(
			pos-int64(rawRecord.FullSize),
			rawRecord.FullSize,
		))
	}

	if _, err := t.markRecordsDeleted(deletableRecords); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	for _, rawRecord := range rawRecords {
		updatedRecord := make(map[string]interface{})
		for col, v := range rawRecord.Record {
			if updatedVal, ok := values[col]; ok {
				updatedRecord[col] = updatedVal
			} else {
				updatedRecord[col] = v
			}
		}
		if _, err := t.Insert(updatedRecord, false); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
	}
	return len(rawRecords), nil
}

func (t *Table) Delete(whereStmt map[string]interface{}) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	if err := t.validateWhereStmt(whereStmt); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}

	deletableRecords := make([]*DeletableRecord, 0)
	for {
		if err := t.recordParser.Parse(); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		rawRecord := t.recordParser.Value
		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		if !t.evaluateWhereStmt(whereStmt, rawRecord.Record) {
			continue
		}

		pos, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}
		deletableRecords = append(deletableRecords, newDeletableRecord(
			pos-int64(rawRecord.FullSize),
			rawRecord.FullSize,
		))
	}
	return t.markRecordsDeleted(deletableRecords)
}

func (t *Table) RestoreWAL() error {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}

	restorableData, err := t.wal.GetRestorableData()
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	// Nothing to restore
	if restorableData == nil {
		fmt.Printf("RestoreWAL skipped\n")
		return nil
	}

	n, err := t.file.Write(restorableData.Data)
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	if n != len(restorableData.Data) {
		return fmt.Errorf("Table.RestoreWAL: %w", columnio.NewIncompleteWriteError(len(restorableData.Data), n))
	}

	fmt.Printf("RestoreWAL wrote %d bytes\n", n)

	if err = t.wal.Commit(restorableData.LastEntry); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}

	return nil
}

func (t *Table) markRecordsDeleted(deletableRecords []*DeletableRecord) (n int, e error) {
	for _, rec := range deletableRecords {
		if _, err := t.file.Seek(rec.offset, io.SeekStart); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		if err := binary.Write(t.file, binary.LittleEndian, types.TypeDeletedRecord); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		length, err := t.reader.ReadUint32()
		if err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		zeroBytes := make([]byte, length)
		if err = binary.Write(t.file, binary.LittleEndian, zeroBytes); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
	}
	return len(deletableRecords), nil
}

func (t *Table) createTmpFile() (*os.File, error) {
	parts := strings.Split(t.file.Name(), ".")
	if len(parts) != 2 {
		return nil, NewInvalidFilename(t.file.Name())
	}
	parts[0] = parts[0] + "_tmp"
	tmpFilename := strings.Join(parts, ".")
	tmpFile, err := os.Create(tmpFilename)
	if err != nil {
		return nil, fmt.Errorf("Table.createTmpFile: %w", err)
	}
	return tmpFile, nil
}

func (t *Table) ensureFilePointer() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	if err := t.seekUntil(types.TypeRecord); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	return nil
}

func (t *Table) ensureColumnLength(record map[string]interface{}) error {
	if len(record) != len(t.columns) {
		return column.NewMismatchingColumnsError(len(t.columns), len(record))
	}
	return nil
}

func (t *Table) evaluateWhereStmt(whereStmt map[string]interface{}, record map[string]interface{}) bool {
	for k, v := range whereStmt {
		if record[k] != v {
			return false
		}
	}
	return true
}

func (t *Table) validateWhereStmt(whereStmt map[string]interface{}) error {
	for k, _ := range whereStmt {
		if !slices.Contains(t.columnNames, k) {
			return fmt.Errorf("unknwon column in where statement: %s", k)
		}
	}
	return nil
}

// ReadRaw returns the raw byte array stored in the table. It's for debugging
func (t *Table) ReadRaw() ([]byte, error) {
	_, err := t.file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("Table.ReadRaw: %w", err)
	}

	stat, err := t.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("Table.ReadRaw: %w", err)
	}

	buf := make([]byte, stat.Size())
	if _, err = t.file.Read(buf); err != nil {
		return nil, fmt.Errorf("Table.ReadRaw: %w", err)
	}
	return buf, nil
}

func (t *Table) ReadRawWAL() ([]byte, error) {
	return t.wal.ReadRaw()
}

func (t *Table) ReadRawLastIDWAL() ([]byte, error) {
	return t.wal.ReadLastCommitRaw()
}

func (t *Table) removeBytes(f *os.File, n uint32) (e error) {
	parts := strings.Split(f.Name(), ".")
	if len(parts) != 2 {
		return NewInvalidFilename(f.Name())
	}
	parts[0] = parts[0] + "_tmp"
	tmpFilename := strings.Join(parts, ".")

	tmpFile, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		e = os.Remove(tmpFile.Name())
	}()

	startPos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Removing %d bytes starting at position %d", n, startPos)

	// Copy content before the deleted bytes
	nCopied, err := io.Copy(tmpFile, io.NewSectionReader(f, 0, startPos))
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Content from 0..%d copied into tmp file (%d bytes)", startPos, nCopied)

	// Skip the deleted bytes
	skipPos, err := f.Seek(int64(n), io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("File pointer is set to %d", skipPos)

	// Copy content after the deleted bytes
	nCopied, err = io.Copy(tmpFile, f)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Content (%d bytes) after position %d is copied to tmp file", nCopied, skipPos)

	// Seek back files before copying tmp into f
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Files seeked back to the beginning")

	// Copy back tmp (that contains the table without the deleted row) to original
	wn, err := io.Copy(f, tmpFile)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Content (%d bytes) copied back to original file", wn)

	newPos, err := f.Seek(startPos, io.SeekStart)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("File pointer is set to %d", newPos)

	tmpPos, err := tmpFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("Truncating file from position %d", tmpPos)

	// Truncate remaining content
	if err = f.Truncate(tmpPos); err != nil {
		return fmt.Errorf("Table.removeBytes: %w", err)
	}
	log.Printf("File truncated")

	return nil
}

func (t *Table) validateColumns(columns map[string]interface{}) error {
	for col, val := range columns {
		if _, ok := t.columns[col]; !ok {
			return fmt.Errorf("Table.validateColumns: %w", column.NewUnknownColumnError(t.Name, col))
		}
		if !t.columns[col].Opts.AllowNull && val == nil {
			return fmt.Errorf("Table.validateColumns: %w", column.NewCannotBeNullError(col))
		}
	}
	return nil
}

func GetTableName(f *os.File) (string, error) {
	// path/to/db/table.bin
	parts := strings.Split(f.Name(), ".")
	if len(parts) != 2 {
		return "", NewInvalidFilename(f.Name())
	}
	filenameParts := strings.Split(parts[0], "/")
	if len(filenameParts) == 0 {
		return "", NewInvalidFilename(f.Name())
	}
	return filenameParts[len(filenameParts)-1], nil
}
