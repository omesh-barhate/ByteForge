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
	"github.com/omesh-barhate/ByteForge/internal/table/index"
	"github.com/omesh-barhate/ByteForge/internal/table/wal"
	walencoding "github.com/omesh-barhate/ByteForge/internal/table/wal/encoding"
)

const (
	FileExtension = ".bin"
	PageSize      = 128
)

type Columns map[string]*column.Column

type Table struct {
	Name        string
	file        *os.File
	columnNames []string
	columns     Columns

	reader          *platformio.Reader
	recordParser    *parser.RecordParser
	columnDefReader *columnio.ColumnDefinitionReader

	index *index.Index
	wal   *wal.WAL
}

func NewTable(
	f *os.File,
	idxFile *os.File,
	reader *platformio.Reader,
	columnDefReader *columnio.ColumnDefinitionReader,
	wal *wal.WAL,
) (*Table, error) {
	if f == nil || reader == nil || columnDefReader == nil || idxFile == nil {
		return nil, fmt.Errorf("NewTable: nil argument")
	}
	tableName, err := getTableName(f)
	if err != nil {
		return nil, fmt.Errorf("NewTable: %w", err)
	}
	t := &Table{
		file:            f,
		Name:            tableName,
		columns:         make(Columns),
		reader:          reader,
		columnDefReader: columnDefReader,
		index:           index.NewIndex(idxFile),
		wal:             wal,
	}
	return t, nil
}

func NewTableWithColumns(
	f *os.File,
	idxFile *os.File,
	reader *platformio.Reader,
	columnDefReader *columnio.ColumnDefinitionReader,
	wal *wal.WAL,
	columns Columns,
	columnNames []string,
) (*Table, error) {
	t, err := NewTable(f, idxFile, reader, columnDefReader, wal)
	if err != nil {
		return nil, fmt.Errorf("NewTableWithColumns: %w", err)
	}
	if err = t.SetColumns(columns, columnNames); err != nil {
		return nil, fmt.Errorf("NewTableWithColumns: %w", err)
	}
	return t, nil
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
		return fmt.Errorf("Table.SetRecordParser: record reader cannot be nil")
	}
	t.recordParser = recParser
	return nil
}

// Close closes the table and the index files
func (t *Table) Close() error {
	if err := t.file.Close(); err != nil {
		return fmt.Errorf("Table.Close: %w", err)
	}
	if err := t.index.Close(); err != nil {
		return fmt.Errorf("Table.Close: %w", err)
	}
	return nil
}

func (t *Table) WriteColumnDefinitions() error {
	for _, v := range t.columnNames {
		b, err := t.columns[v].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: MarshalBinary: %w", err)
		}

		colWriter := columnio.NewColumnDefinitionWriter(t.file)
		if _, err = colWriter.Write(b); err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: Write: %w", err)
		}
	}
	return nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: file.Seek: %w", err)
	}

	for {
		buf := make([]byte, 4096)
		n, err := t.columnDefReader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		col := column.Column{}
		err = col.UnmarshalBinary(buf[:n])
		if err != nil {
			return fmt.Errorf("Table.ReadColumnDefinitions: UnmarshalBinary: %w", err)
		}
		colName := col.NameToStr()
		t.columns[colName] = &col
		t.columnNames = append(t.columnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]interface{}, useWAL bool) (int, error) {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: file.Seek %w", err)
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

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeRecord); err != nil {
		return 0, fmt.Errorf("Table.Insert: type: %w", err)
	}

	// length of whole record
	if err := binary.Write(&buf, binary.LittleEndian, sizeOfRecord); err != nil {
		return 0, fmt.Errorf("Table.Insert: len: %w", err)
	}

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
		var err error
		walEntry, err = t.wal.Append(walencoding.OpInsert, t.Name, buf.Bytes())
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
	}

	page, err := t.insertIntoPage(buf)
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if err = t.index.AddAndPersist(record["id"].(int64), page.StartPos); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	if useWAL {
		if err = t.wal.Commit(walEntry); err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
	}

	return 1, nil
}

// insertIntoPage finds the first page that can fit buf and writes it into the page
func (t *Table) insertIntoPage(buf bytes.Buffer) (*index.Page, error) {
	page, err := t.seekToNextPage(uint32(buf.Len()))
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: %w", err)
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, columnio.NewIncompleteWriteError(buf.Len(), n)
	}

	// seek back to the beginning of page
	if _, err = t.file.Seek(page.StartPos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Seek: %w", err)
	}
	return page, t.updatePageSize(page.StartPos, int32(buf.Len()))
}

// seekToNextPage seeks the file to the first page that can hold lenToFit number of bytes
func (t *Table) seekToNextPage(lenToFit uint32) (*index.Page, error) {
	_, err := t.file.Seek(0, io.SeekStart)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
	}
	for {
		err = t.seekUntil(types.TypePage)
		if err != nil {
			if err == io.EOF {
				return t.insertEmptyPage()
			}
			return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
		}
		// Skipping the type definition byte
		if _, err = t.reader.ReadByte(); err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readByte: %w", err)
		}
		currPageLen, err := t.reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readUint32: %w", err)
		}
		if currPageLen+lenToFit <= PageSize {
			meta := int64(types.LenByte + types.LenInt32)
			pagePos, err := t.file.Seek(-1*meta, io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("Table.seekToNextPage: file.Seek: %w", err)
			}
			_, err = t.file.Seek(int64(currPageLen)+meta, io.SeekCurrent)
			return index.NewPage(pagePos), err
		}
	}
}

func (t *Table) insertEmptyPage() (*index.Page, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypePage); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: type: %w", err)
	}

	// length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: len: %w", err)
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, columnio.NewIncompleteWriteError(buf.Len(), n)
	}

	currPos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: %w", err)
	}
	// startPos should point at the very first byte, that is types.TypePage and 5 bytes before the current pos
	startPos := currPos - (types.LenInt32 + types.LenByte)
	if startPos <= 0 {
		return nil, fmt.Errorf("Table.insertEmptyPage: unable to insert new page: start should be positive: %d", startPos)
	}
	return index.NewPage(startPos), nil
}

// seekUntil finds the first occurrence of targetType and seeks the file to it's position
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
			if _, err = t.file.Seek(-1, io.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: file.Seek: %w", err)
			}
			return nil
		}

		if targetType == types.TypeRecord && dataType == types.TypePage {
			// Ignore page's len
			if _, err := t.reader.ReadUint32(); err != nil {
				return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
			}
			// The first type flag inside a page should be a record
			dataType, err = t.skipDeletedRecords()
			//dataType, err = t.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("Table.seekUntil: readByte: %w", err)
			}
			if dataType != targetType {
				return fmt.Errorf("Table.seekUntil: first byte inside a page should be %d but %d found", types.TypeRecord, dataType)
			}
			if _, err = t.file.Seek(-1, io.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: file.Seek: %w", err)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
		}
		if _, err = t.file.Seek(int64(length), io.SeekCurrent); err != nil {
			return fmt.Errorf("Table.seekUntil: file.Seek: %w", err)
		}
	}
}

func (t *Table) skipDeletedRecords() (dataType byte, err error) {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return 0, err
			}
			return 0, fmt.Errorf("Table.skipDeletedRecords: %w", err)
		}
		if dataType == types.TypeDeletedRecord {
			l, err := t.reader.ReadUint32()
			if err != nil {
				return 0, fmt.Errorf("RecordParser.Parse: %w", err)
			}
			if _, err = t.file.Seek(int64(l), io.SeekCurrent); err != nil {
				return 0, fmt.Errorf("RecordParser.Parse: %w", err)
			}
		}
		if dataType == types.TypeRecord {
			return dataType, nil
		}
	}
}

func (t *Table) Select(whereStmt map[string]interface{}) ([]map[string]interface{}, error) {
	if err := t.ensureFilePointer(); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}

	results := make([]map[string]interface{}, 0)
	fields := make([]string, 0)
	for k, _ := range whereStmt {
		fields = append(fields, k)
	}

	singleResult := false

	// use the index to seek the file to the right page
	if slices.Contains(fields, "id") {
		singleResult = true
		item, err := t.index.Get(whereStmt["id"].(int64))
		if err == nil {
			if _, err = t.file.Seek(item.PagePos, io.SeekStart); err != nil {
				return nil, fmt.Errorf("Table.Select: %w", err)
			}
			pr := platformio.NewPageReader(t.reader)
			pageContent := make([]byte, PageSize+types.LenMeta)

			n, err := pr.Read(pageContent)
			if err != nil {
				return nil, fmt.Errorf("Table.Select: %w", err)
			}

			pageContent = pageContent[:n]
			reader := bytes.NewReader(pageContent)
			t.recordParser = parser.NewRecordParser(reader, t.ColumnNames())
		}
	}

	defer func() {
		t.recordParser = parser.NewRecordParser(t.file, t.ColumnNames())
	}()

	for {
		err := t.recordParser.Parse()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Table.Select: parse: %w", err)
		}
		rawRecord := t.recordParser.Value

		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return nil, column.NewMismatchingColumnsError(len(t.columns), len(rawRecord.Record))
		}

		if !t.evaluateWhereStmt(whereStmt, rawRecord.Record) {
			continue
		}

		result := make(map[string]interface{})
		for _, col := range t.columnNames {
			result[col] = rawRecord.Record[col]
		}
		results = append(results, result)

		if singleResult {
			return results, nil
		}
	}
}

func (t *Table) Update(whereStmt map[string]interface{}, values map[string]interface{}) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}
	if err := t.validateColumns(values); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	rawRecords, err := t.delete(whereStmt)
	if err != nil {
		return 0, fmt.Errorf("table.Update: %w", err)
	}

	for _, rawRecord := range rawRecords {
		updatedRecord := make(map[string]interface{})
		for k, v := range rawRecord.Record {
			if updatedVal, ok := values[k]; ok {
				updatedRecord[k] = updatedVal
			} else {
				updatedRecord[k] = v
			}
		}
		if _, err = t.Insert(updatedRecord, false); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
	}
	return len(rawRecords), nil
}

func (t *Table) Delete(whereStmt map[string]interface{}) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	records, err := t.delete(whereStmt)
	if err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	return len(records), nil
}

func (t *Table) LoadIdx() error {
	return t.index.Load()
}

// removeRecords removes the given records by using a tmp file:
//   - Copies the content before the first target to the tmp file
//   - Locates the targets and skips them
//   - Copies everything between the target
//   - Copies everything after the last target till EOF
//   - Then it copies everything from the tmp file (which doesn't contain the deleted records at this point) back to the table file
func (t *Table) removeRecords(recordsToDelete []*DeletableRecord) (n int, e error) {
	if len(recordsToDelete) == 0 {
		return 0, nil
	}

	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: %w", err)
	}

	tmpFile, err := t.createTmpFile()
	if err != nil {
		return 0, fmt.Errorf("Table.removeRecords: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		e = os.Remove(tmpFile.Name())
	}()

	// copy everything before the first target
	wn, err := io.Copy(tmpFile, io.NewSectionReader(t.file, 0, recordsToDelete[0].pos))
	if err != nil {
		return 0, fmt.Errorf("Table.removeRecords: io.Copy: %w", err)
	}
	log.Printf("copy 0..%d written %d", recordsToDelete[0].pos, wn)

	// seek to the first target
	offset, err := t.file.Seek(recordsToDelete[0].pos, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("Table.removeRecords: file.Seek: %w", err)
	}
	log.Printf("position after io.Copy: %d", offset)

	i := 0
	for {
		if i >= len(recordsToDelete) {
			break
		}

		// skip target
		offset, err = t.file.Seek(int64(recordsToDelete[i].len), io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.removeRecords: file.Seek: %w", err)
		}
		log.Printf("file seeked at %d", offset)

		// we're done. copy the remaining content til EOF
		if i == len(recordsToDelete)-1 {
			if _, err = io.Copy(tmpFile, t.file); err != nil {
				return 0, fmt.Errorf("Table.removeRecords: io.Copy: %w", err)
			}
			break
		}

		// copy content til the next target
		wn, err = io.Copy(tmpFile, io.NewSectionReader(t.file, offset, recordsToDelete[i+1].pos-offset))
		if err != nil {
			return 0, fmt.Errorf("Table.removeRecords: io.Copy: %w", err)
		}
		log.Printf("copy %d..%d written %d", offset, recordsToDelete[i+1].pos, wn)
		i++
	}

	// Overwriting table file with tmp file
	if _, err = t.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: file.Seek: %w", err)
	}
	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: file.Seek tmpFile: %w", err)
	}
	if err = t.file.Truncate(0); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: file.Truncate: %w", err)
	}
	if _, err = io.Copy(t.file, tmpFile); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: io.Copy tmpFile: %w", err)
	}
	if _, err = t.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("Table.removeRecords: file.Seek: %w", err)
	}

	return i + 1, nil
}

// createTmpFile created a temp file for the table. It is used for delete and update
func (t *Table) createTmpFile() (*os.File, error) {
	parts := strings.Split(t.file.Name(), ".")
	if len(parts) != 2 {
		return nil, NewInvalidFilename(t.file.Name())
	}
	parts[0] = parts[0] + "_tmp"
	tmpFilename := strings.Join(parts, ".")
	tmpFile, err := os.Create(tmpFilename)
	if err != nil {
		return nil, fmt.Errorf("Table.createTmpFile: os.Create: %w", err)
	}
	return tmpFile, nil
}

// ensureFilePointer seeks the file pointer the to first record
func (t *Table) ensureFilePointer() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ensureFilePointer: file.Seek: %w", err)
	}

	if err := t.seekUntil(types.TypeRecord); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}

	return nil
}

// ensureColumnLength checks if the given map has the same amount of columns as the table definition
// It is used to check raw records read from the table file
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

// pagePositions returns all the page positions in the table file as a slice
func (t *Table) pagePositions() (pos []int64, e error) {
	positions := make([]int64, 0)
	origPos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("table.pagePositions: file.Seek: %w", err)
	}
	defer func() {
		if _, err = t.file.Seek(origPos, io.SeekStart); err != nil {
			e = fmt.Errorf("Table.pagePositions: seeking back to original position: %w", err)
		}
	}()

	if _, err = t.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Table.pagePositions: file.Seek: %w", err)
	}

	if err := t.seekUntil(types.TypePage); err != nil {
		if err == io.EOF {
			return []int64{}, nil
		}
		return nil, fmt.Errorf("Table.pagePositions: %w", err)
	}

	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return positions, nil
			}
			return nil, fmt.Errorf("Table.pagePositions: readByte: %w", err)
		}
		if dataType != types.TypePage {
			return positions, nil
		}
		p, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("Table.pagePositions: file.Seek: %w", err)
		}
		// the type byte has already been read, so we need to subtract 1 from the current position
		positions = append(positions, p-1)

		length, err := t.reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("Table.pagePositions: readUint32: %w", err)
		}
		if _, err = t.file.Seek(int64(length), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("Table.pagePositions: file.Seek: %w", err)
		}
	}
}

// findContainingPage returns the position of the page that contains the given position
//
//	 Example
//		pagePositions = [430,558,686]
//	    pos = 602
//	    result = 558
//
// The result is 558 because position 602 can be found in the page located between 558..686
func findContainingPage(pagePositions []int64, pos int64) (int64, error) {
	if len(pagePositions) == 0 {
		return -1, NewPageNotFoundError(pagePositions, pos)
	}
	if pagePositions[0] > pos {
		return -1, NewPageNotFoundError(pagePositions, pos)
	}
	var left int64
	right := int64(len(pagePositions) - 1)
	for left < right {
		mid := left + (right-left+1)/2
		if pagePositions[mid] < pos {
			left = mid
		} else {
			right = mid - 1
		}
	}
	return pagePositions[left], nil
}

// updatePageSize increases or decreases the size of a page by offset
// if the new size is 0 the page is removed
func (t *Table) updatePageSize(page int64, offset int32) (e error) {
	origPos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	defer func() {
		_, err := t.file.Seek(origPos, io.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, io.SeekStart); err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}

	dataType, err := t.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	if dataType != types.TypePage {
		return fmt.Errorf("Table.updatePageSize: file pointer is at wrong position: expected: %d, actual: %d", types.TypePage, dataType)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	_, err = t.file.Seek(-1*types.LenInt32, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}

	var newLength uint32
	if offset >= 0 {
		newLength = length + uint32(offset)
	} else {
		newLength = length - uint32(-offset)
	}

	marshaler := encoding.NewValueMarshaler[uint32](newLength)
	b, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	n, err := t.file.Write(b)
	if n != len(b) {
		return columnio.NewIncompleteWriteError(len(b), n)
	}

	if newLength == 0 {
		if err = t.removeEmptyPage(page); err != nil {
			return fmt.Errorf("Table.updatePageSize: %w", err)
		}
	}
	return nil
}

func (t *Table) removeEmptyPage(page int64) (e error) {
	origPos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	defer func() {
		_, err := t.file.Seek(origPos, io.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, io.SeekStart); err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	dataType, err := t.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	if dataType != types.TypePage {
		return fmt.Errorf("Table.removePage: file pointer points to invalid byte: %d", dataType)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	if length != 0 {
		return NewPageNotEmptyError(page, length)
	}
	stat, err := t.file.Stat()
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	beforeReader := io.NewSectionReader(t.file, 0, page)
	afterReader := io.NewSectionReader(t.file, page+types.LenByte+types.LenInt32, stat.Size())
	beforeBuf := make([]byte, page)
	if _, err = beforeReader.Read(beforeBuf); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	afterBuf := make([]byte, stat.Size()-(page+types.LenByte+types.LenInt32))
	if _, err = afterReader.Read(afterBuf); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	if _, err = t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	bw, err := t.file.Write(beforeBuf)
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	aw, err := t.file.Write(afterBuf)
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	if err = t.file.Truncate(int64(bw + aw)); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	return nil
}

func (t *Table) delete(whereStmts map[string]interface{}) ([]*parser.RawRecord, error) {
	rawRecords := make([]*parser.RawRecord, 0)
	recordsToDelete := make([]*DeletableRecord, 0)
	for {
		if err := t.recordParser.Parse(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("Table.delete: %w", err)
		}

		rawRecord := t.recordParser.Value
		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return nil, fmt.Errorf("Table.delete: %w", err)
		}

		if !t.evaluateWhereStmt(whereStmts, rawRecord.Record) {
			continue
		}

		rawRecords = append(rawRecords, rawRecord)
		log.Printf("Eligable for deletion: %v\n", rawRecord)
		pos, err := t.file.Seek(-1*int64(rawRecord.FullSize), io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("Table.delete: %w", err)
		}
		id, err := rawRecord.Id()
		if err != nil {
			return nil, fmt.Errorf("Table.delete: %w", err)
		}
		recordsToDelete = append(recordsToDelete, NewDeletableRecord(id, pos, rawRecord.FullSize))
		if _, err = t.file.Seek(int64(rawRecord.FullSize), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("Table.delete: %w", err)
		}
	}

	if _, err := t.markRecordsDeleted(recordsToDelete); err != nil {
		return nil, fmt.Errorf("Table.delete: %w", err)
	}

	ids := make([]int64, len(recordsToDelete))
	for _, v := range recordsToDelete {
		ids = append(ids, v.id)
	}
	if err := t.index.RemoveManyAndPersist(ids); err != nil {
		return nil, fmt.Errorf("Table.delete: %w", err)
	}
	return rawRecords, nil
}

func (t *Table) validateColumns(columns map[string]interface{}) error {
	for col, val := range columns {
		if _, ok := t.columns[col]; !ok {
			return column.NewUnknownColumnError(t.Name, col)
		}
		if !t.columns[col].Opts.AllowNull && val == nil {
			return column.NewCannotBeNullError(col)
		}
	}
	return nil
}

func getTableName(f *os.File) (string, error) {
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

func (t *Table) markRecordsDeleted(deletableRecords []*DeletableRecord) (n int, e error) {
	for _, rec := range deletableRecords {
		if _, err := t.file.Seek(rec.pos, io.SeekStart); err != nil {
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

	//fmt.Printf("RestoreWAL wrote %d bytes\n", n)

	if err = t.wal.Commit(restorableData.LastEntry); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}

	return nil
}

// ---- Debug ----

// ReadRaw returns the raw byte array stored in the table. It's for debugging
func (t *Table) ReadRaw() ([]byte, error) {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
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

// ReadRawIdx returns the raw byte array stored in the table. It's for debugging
func (t *Table) ReadRawIdx() ([]byte, error) {
	return t.index.ReadRaw()
}

type DeletableRecord struct {
	id  int64
	pos int64
	len uint32
}

func NewDeletableRecord(id, pos int64, len uint32) *DeletableRecord {
	return &DeletableRecord{
		id:  id,
		pos: pos,
		len: len,
	}
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
