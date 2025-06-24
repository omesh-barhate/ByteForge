package parser

import (
	"fmt"
	"io"

	platformio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type RecordParser struct {
	file    io.ReadSeeker
	columns []string
	Value   *RawRecord
	reader  *platformio.Reader
}

func NewRecordParser(f io.ReadSeeker, columns []string) *RecordParser {
	return &RecordParser{
		file:    f,
		columns: columns,
	}
}

func (r *RecordParser) Parse() error {
	read, err := platformio.NewReader(r.file)
	if err != nil {
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}
	r.reader = read

	t, err := read.ReadByte()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}
	if t == types.TypePage {
		if _, err = read.ReadUint32(); err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		t, err = read.ReadByte()
		if err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
	}
	if t != types.TypeRecord && t != types.TypeDeletedRecord {
		return fmt.Errorf("RecordParser.Parse: unable to read record: file offset needs to point at the record definition: %v", t)
	}

	if t == types.TypeDeletedRecord {
		if _, err := r.file.Seek(-1*types.LenByte, io.SeekCurrent); err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		err = r.skipDeletedRecords()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
	}

	record := make(map[string]interface{})
	lenRecord, err := read.ReadUint32()
	for i := 0; i < len(r.columns); i++ {
		_, err = read.ReadByte()
		if err == io.EOF {
			r.Value = NewRawRecord(
				lenRecord,
				record,
			)
			return nil
		}
		if err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}

		// we need to seek back 1 byte so TLVParser can decode it
		if _, err := r.file.Seek(-1*types.LenByte, io.SeekCurrent); err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}

		tlvParser := NewTLVParser(read)
		value, err := tlvParser.Parse()
		if err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}

		record[r.columns[i]] = value
	}
	r.Value = NewRawRecord(
		lenRecord,
		record,
	)
	return nil
}

func (r *RecordParser) skipDeletedRecords() error {
	for {
		t, err := r.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		if t == types.TypeDeletedRecord {
			l, err := r.reader.ReadUint32()
			if err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
			if _, err = r.file.Seek(int64(l), io.SeekCurrent); err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
		}
		if t == types.TypeRecord {
			return nil
		}
	}
}

// RawRecord represents one record read from the table file
// As the data is stored in TLV format it stores the columns in a slice
type RawRecord struct {
	// Size is the size of the record in bytes. This only includes the actual fields
	Size uint32
	// FullSize is sum of [RawRecord.Size] and [types.LenMeta] that includes metadata associated with a records such as the type and length bytes
	FullSize uint32
	// Values contains the actual fields
	Record map[string]interface{}
}

func NewRawRecord(size uint32, record map[string]interface{}) *RawRecord {
	return &RawRecord{
		Size:     size,
		FullSize: size + types.LenMeta,
		Record:   record,
	}
}

func (r *RawRecord) Id() (int64, error) {
	val, ok := r.Record["id"].(int64)
	if !ok {
		return -1, NewInvalidIDError(r)
	}
	return val, nil
}
