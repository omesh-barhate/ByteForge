package wal

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	platformio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
	walencoding "github.com/omesh-barhate/ByteForge/internal/table/wal/encoding"
)

const (
	FilenameTmpl           = "%s_wal.bin"
	LastCommitFilenameTmpl = "%s_wal_last_commit.bin"
)

type (
	WAL struct {
		file           *os.File
		lastCommitFile *os.File
	}
	Entry struct {
		ID  string
		Len uint32
	}
	RestorableData struct {
		LastEntry *Entry
		Data      []byte
	}
)

func NewWAL(dbPath, tableName string) (*WAL, error) {
	path := filepath.Join(dbPath, fmt.Sprintf(FilenameTmpl, tableName))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		f, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("NewWal: %w", err)
		}
	}

	path = filepath.Join(dbPath, fmt.Sprintf(LastCommitFilenameTmpl, tableName))
	lastIDFile, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		lastIDFile, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("NewWal: %w", err)
		}
	}

	return &WAL{
		file:           f,
		lastCommitFile: lastIDFile,
	}, nil
}

func (w *WAL) Append(op, table string, data []byte) (*Entry, error) {
	id, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	marshaler := walencoding.NewWALMarshaler(id, op, table, data)
	buf, err := marshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	if err = w.write(buf); err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	return newEntry(id, buf), nil
}

func (w *WAL) Commit(entry *Entry) error {
	marshaler := walencoding.NewLastCommitMarshaler(entry.ID, entry.Len)
	data, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}

	if err := os.WriteFile(w.lastCommitFile.Name(), data, 0644); err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}
	return nil
}

func (w *WAL) GetRestorableData() (*RestorableData, error) {
	if _, err := w.lastCommitFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("WAL.GetRestorableData: seek: %w", err)
	}

	data := make([]byte, 1024)
	n, err := w.lastCommitFile.Read(data)
	if err != nil {
		return nil, fmt.Errorf("WAL.GetRestorableData: read: %w", err)
	}

	data = data[:n]
	unmarshaler := walencoding.NewLastCommitUnmarshaler()
	if err = unmarshaler.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("WAL.GetRestorableData: unmarshal: %w", err)
	}
	lastCommittedID := unmarshaler.ID

	lastEntry, err := w.readLastEntry(unmarshaler.Len)
	if err != nil {
		return nil, fmt.Errorf("WAL.GetRestorableData: %w", err)
	}

	if lastEntry.ID == lastCommittedID {
		return nil, nil
	}

	buf, err := w.getRestorableData(lastCommittedID)
	if err != nil {
		return nil, fmt.Errorf("WAL.GetRestorableData: %w", err)
	}

	return newRestorableData(lastEntry, buf), nil
}

func (w *WAL) getRestorableData(commitID string) ([]byte, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
	}

	r, err := platformio.NewReader(w.file)
	if err != nil {
		return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
	}

	commitIDFound := false
	buf := bytes.Buffer{}
	for {
		t, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return buf.Bytes(), nil
			}
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}
		if t != types.TypeWALEntry {
			return nil, fmt.Errorf("WAL.getRestorableData: invalid type")
		}

		length, err := r.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}

		tlvParser := parser.NewTLVParser(r)
		val, err := tlvParser.Parse()
		id := val.(string)

		if err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}

		if id == commitID {
			commitIDFound = true
			if err = w.skipEntry(id, length); err != nil {
				return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
			}
			continue
		}

		// We are before the commit ID so entry can be skipped entirely
		if !commitIDFound {
			if err = w.skipEntry(id, length); err != nil {
				return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
			}
			continue
		}

		// We are after the commit so this entry needs to be restored

		// op
		val, err = tlvParser.Parse()
		op := val.(string)
		if op != walencoding.OpInsert {
			return nil, fmt.Errorf("WAL.getRestorableData: unspoorted operation: %s", op)
		}

		// table
		val, err = tlvParser.Parse()

		// data
		t, err = r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}
		if t != types.TypeRecord {
			return nil, fmt.Errorf("WAL.getRestorableData: invalid type: %d, %d was expected", t, types.TypeRecord)
		}

		length, err = r.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}

		buf.WriteByte(t)
		if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}

		record := make([]byte, length)
		if _, err = r.Read(record); err != nil {
			return nil, fmt.Errorf("WAL.getRestorableData: %w", err)
		}
		buf.Write(record)
	}
}

func (w *WAL) skipEntry(id string, length uint32) error {
	// Seek back to the beginning of the ID
	if _, err := w.file.Seek(-1*(int64(len(id)+types.LenByte+types.LenInt32)), io.SeekCurrent); err != nil {
		return fmt.Errorf("WAL.skipEntry: %w", err)
	}
	// Skip current entry
	if _, err := w.file.Seek(int64(length), io.SeekCurrent); err != nil {
		return fmt.Errorf("WAL.skipEntry: %w", err)
	}
	return nil
}

func (w *WAL) readLastEntry(length uint32) (*Entry, error) {
	if _, err := w.file.Seek(-1*int64(length), io.SeekEnd); err != nil {
		return nil, fmt.Errorf("WAL.readLastEntry: %w", err)
	}

	buf := make([]byte, length)
	n, err := w.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("WAL.readLastEntry: %w", err)
	}
	if n != int(length) {
		return nil, fmt.Errorf("WAL.readLastEntry: incomplete read")
	}

	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	intUnmarshaler := encoding.NewValueUnmarshaler[uint32]()
	bytesRead := 0

	// type
	if err = byteUnmarshaler.UnmarshalBinary(buf); err != nil {
		return nil, fmt.Errorf("WAL.readLastEntry: type: %w", err)
	}
	bytesRead += types.LenByte

	// len
	if err = intUnmarshaler.UnmarshalBinary(buf[bytesRead:]); err != nil {
		return nil, fmt.Errorf("WAL.readLastEntry: len: %w", err)
	}
	bytesRead += types.LenInt32

	strUnmarshaler := encoding.NewValueUnmarshaler[string]()
	tlvUnmarshaler := encoding.NewTLVUnmarshaler(strUnmarshaler)

	// ID
	if err = tlvUnmarshaler.UnmarshalBinary(buf[bytesRead:]); err != nil {
		return nil, fmt.Errorf("WAL.readLastEntry: val: %w", err)
	}
	bytesRead += len(tlvUnmarshaler.Value)
	id := tlvUnmarshaler.Value

	return &Entry{
		ID:  id,
		Len: length,
	}, nil
}

func (w *WAL) write(buf []byte) error {
	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL.write: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("WAL.write: incomplete write. %d bytes written instead of %d", n, len(buf))
	}
	return nil
}

// ReadRaw returns the raw byte array stored in the WAL file. It's for debugging
func (w *WAL) ReadRaw() ([]byte, error) {
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}

	stat, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}

	buf := make([]byte, stat.Size())
	if _, err = w.file.Read(buf); err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}
	return buf, nil
}

// ReadLastCommitRaw returns the raw byte array stored in the last commit file. It's for debugging
func (w *WAL) ReadLastCommitRaw() ([]byte, error) {
	_, err := w.lastCommitFile.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}

	stat, err := w.lastCommitFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}

	buf := make([]byte, stat.Size())
	if _, err = w.lastCommitFile.Read(buf); err != nil {
		return nil, fmt.Errorf("WAL.ReadRaw: %w", err)
	}
	return buf, nil
}

func newEntry(id string, data []byte) *Entry {
	return &Entry{
		ID:  id,
		Len: uint32(len(data)),
	}
}

func newRestorableData(entry *Entry, data []byte) *RestorableData {
	return &RestorableData{
		LastEntry: entry,
		Data:      data,
	}
}

func generateID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("wal.generateID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
