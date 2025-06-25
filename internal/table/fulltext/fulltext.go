package fulltext

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"

	platformencoding "github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type Index struct {
	hMap map[string][]*IndexItem
	file *os.File
}

func NewIndex(f *os.File) *Index {
	return &Index{
		hMap: make(map[string][]*IndexItem),
		file: f,
	}
}

type IndexItem struct {
	PagePos int64
	ID      int64
}

func NewIndexItem(page, id int64) *IndexItem {
	return &IndexItem{
		PagePos: page,
		ID:      id,
	}
}

func (idx *Index) Add(word string, page, id int64) {
	if word == "" {
		return
	}
	item := NewIndexItem(page, id)
	_, ok := idx.hMap[word]
	if !ok {
		idx.hMap[word] = make([]*IndexItem, 0)
	}
	idx.hMap[word] = append(idx.hMap[word], item)
}

func (idx *Index) AddAndPersist(word string, page, id int64) error {
	idx.Add(word, page, id)
	if err := idx.persist(); err != nil {
		return fmt.Errorf("fulltext.index.AddAndPersist: %w", err)
	}
	return nil
}

func (idx *Index) Get(word string) ([]*IndexItem, error) {
	val, ok := idx.hMap[word]
	if !ok {
		return nil, ErrItemNotFound
	}
	return val, nil
}

func (idx *Index) RemoveMany(ids []int64) {
	if len(ids) == 0 {
		return
	}
	for w, _ := range idx.hMap {
		idx.hMap[w] = slices.DeleteFunc(idx.hMap[w], func(item *IndexItem) bool {
			return slices.Contains(ids, item.ID)
		})
		if len(idx.hMap[w]) == 0 {
			delete(idx.hMap, w)
		}
	}
}

func (idx *Index) RemoveManyAndPersist(ids []int64) error {
	idx.RemoveMany(ids)
	if err := idx.persist(); err != nil {
		return fmt.Errorf("fulltext.index.RemoveManyAndPersist: %w", err)
	}
	return nil
}

func (idx *Index) persist() error {
	if err := idx.file.Truncate(0); err != nil {
		return fmt.Errorf("fulltext.index.persist: %w", err)
	}
	if _, err := idx.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("fulltext.index.persist: %w", err)
	}

	b, err := idx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("fulltext.index.persist: %w", err)
	}
	n, err := idx.file.Write(b)
	if err != nil {
		return fmt.Errorf("fulltext.index.persist: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("fulltext.index.persist: incopmlete write")
	}
	return nil
}

func (idx *Index) Load() error {
	if _, err := idx.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("fulltext.index.Load: %w", err)
	}
	stat, err := idx.file.Stat()
	if err != nil {
		return fmt.Errorf("fulltext.index.Load: %w", err)
	}
	b := make([]byte, stat.Size())
	n, err := idx.file.Read(b)
	if err != nil {
		return fmt.Errorf("fulltext.index.Load: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("fulltext.index.Load: incomplete read")
	}
	if err = idx.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("fulltext.index.Load: %w", err)
	}
	return nil
}

func (idx *Index) Close() error {
	return idx.file.Close()
}

func (idx *Index) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeIndex); err != nil {
		return nil, fmt.Errorf("fulltext.index.MarshalBinary: idx type: %w", err)
	}

	hMapMarshaler := platformencoding.NewHMapMarshaler(convertHMap(idx.hMap))
	hMapBytes, err := hMapMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("fulltext.index.MarshalBinary: %w", err)
	}

	// len
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(hMapBytes))); err != nil {
		return nil, fmt.Errorf("fulltext.index.MarshalBinary: idx len: %w", err)
	}

	buf.Write(hMapBytes)
	return buf.Bytes(), nil
}

func (idx *Index) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := platformencoding.NewValueUnmarshaler[byte]()
	int32Unmarshaler := platformencoding.NewValueUnmarshaler[uint32]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("fulltext.Index.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("fulltext.Index.UnmarshalBinary: len: %w", err)
	}
	n += types.LenInt32

	hmapUnmarshaler := platformencoding.NewHMapUnmarshaler(func() platformencoding.EmbeddedValueUnmarshaler {
		return &IndexItem{}
	})

	if err := hmapUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("fulltext.index.UnmarshalBinary: hmap: %w", err)
	}

	for key, items := range hmapUnmarshaler.Value {
		switch v := items.(type) {
		case []platformencoding.EmbeddedValueUnmarshaler:
			for _, item := range v {
				val := item.GetValue()
				switch itemVal := val.(type) {
				case map[string]int64:
					idx.Add(key, itemVal["page"], itemVal["id"])
				default:
					return fmt.Errorf("fulltext.index.UnmarshalBinary: hMap unmarshaler has invalid item type. want: map[string]int64, have: %T", itemVal)
				}
			}

		default:
			return fmt.Errorf("fulltext.index.UnmarshalBinary: hMap unmarshaler has invalid type. want: []interface{...}, have: %T", v)
		}
	}
	return nil
}

func (item *IndexItem) BinaryLen() uint32 {
	return uint32(binary.Size(item)) + // two integers = 16
		(2 * types.LenMeta) // 10 meta bytes for the two ints = 10
}

func (item *IndexItem) GetValue() interface{} {
	return map[string]int64{
		"id":   item.ID,
		"page": item.PagePos,
	}
}

func (item *IndexItem) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeIndexItem); err != nil {
		return nil, fmt.Errorf("fulltext.indexItem.MarshalBinary: type: %w", err)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, item.BinaryLen()); err != nil {
		return nil, fmt.Errorf("fulltext.indexItem.MarshalBinary: len: %w", err)
	}
	idTLV := platformencoding.NewTLVMarshaler(item.ID)
	idBuf, err := idTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("fulltext.indexItem.MarshalBinary: ID: %w", err)
	}
	buf.Write(idBuf)

	pageTLV := platformencoding.NewTLVMarshaler(item.PagePos)
	pageBuf, err := pageTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("fulltext.indexItem.MarshalBinary: page: %w", err)
	}
	buf.Write(pageBuf)
	return buf.Bytes(), nil
}

func (item *IndexItem) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := platformencoding.NewValueUnmarshaler[byte]()
	int32Unmarshaler := platformencoding.NewValueUnmarshaler[uint32]()
	int64Unmarshaler := platformencoding.NewValueUnmarshaler[int64]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("fulltext.IndexItem.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("fulltext.IndexItem.UnmarshalBinary: len: %w", err)
	}
	n += types.LenInt32

	idTLV := platformencoding.NewTLVUnmarshaler[int64](int64Unmarshaler)
	if err := idTLV.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("fulltext.IndexItem.UnmarshalBinary: ID: %w", err)
	}
	n += int(idTLV.BytesRead)
	id := idTLV.Value

	pageTLV := platformencoding.NewTLVUnmarshaler[int64](int64Unmarshaler)
	if err := pageTLV.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("fulltext.IndexItem.UnmarshalBinary: ID: %w", err)
	}
	n += int(pageTLV.BytesRead)
	page := pageTLV.Value

	item.ID = id
	item.PagePos = page
	return nil
}

// It returns a map that is compatible with platformencoding.HMapMarshaler
// It does not modify values
func convertHMap(hMap map[string][]*IndexItem) map[string][]platformencoding.EmbeddedValueMarshaler {
	convertedMap := make(map[string][]platformencoding.EmbeddedValueMarshaler)
	for k, v := range hMap {
		convertedSlice := make([]platformencoding.EmbeddedValueMarshaler, len(v))
		for i, item := range v {
			convertedSlice[i] = platformencoding.EmbeddedValueMarshaler(item)
		}
		convertedMap[k] = convertedSlice
	}
	return convertedMap
}

// ReadRaw returns the raw byte array stored in the idx. It's for debugging
func (idx *Index) ReadRaw() ([]byte, error) {
	if _, err := idx.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("index.ReadRaw: %w", err)
	}

	stat, err := idx.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("index.ReadRaw: %w", err)
	}

	buf := make([]byte, stat.Size())
	if _, err = idx.file.Read(buf); err != nil {
		return nil, fmt.Errorf("index.ReadRaw: %w", err)
	}
	return buf, nil
}
