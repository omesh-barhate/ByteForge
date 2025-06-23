package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/google/btree"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type Index struct {
	btree *btree.BTreeG[Item]
	file  *os.File
}

func NewIndex(f *os.File) *Index {
	bt := btree.NewG[Item](2, func(a, b Item) bool {
		return a.id < b.id
	})
	return &Index{
		btree: bt,
		file:  f,
	}
}

func (i *Index) Close() error {
	return i.file.Close()
}

func (i *Index) AddAndPersist(id, pagePos int64) error {
	i.btree.ReplaceOrInsert(*NewItem(id, pagePos))
	return i.persist()
}

func (i *Index) RemoveManyAndPersist(ids []int64) error {
	for _, id := range ids {
		i.btree.Delete(Item{id: id})
	}
	if err := i.persist(); err != nil {
		return fmt.Errorf("index.RemoveManyAndPersist: %w", err)
	}
	return nil
}

func (i *Index) Add(id, pagePos int64) {
	i.btree.ReplaceOrInsert(*NewItem(id, pagePos))
}

func (i *Index) Get(id int64) (Item, error) {
	item, ok := i.btree.Get(Item{id: id})
	if !ok {
		return Item{}, NewItemNotFoundError(id)
	}
	return item, nil
}

func (i *Index) persist() error {
	if err := i.file.Truncate(0); err != nil {
		return fmt.Errorf("index.persist: file.Truncate: %w", err)
	}
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("index.persist: file.Seek: %w", err)
	}

	b, err := i.MarshalBinary()
	if err != nil {
		return fmt.Errorf("index.persist: marshalBinary: %w", err)
	}
	n, err := i.file.Write(b)
	if err != nil {
		return fmt.Errorf("index.persist: file.Write: %w", err)
	}
	if n != len(b) {
		return NewIncompleteWriteError(len(b), n)
	}
	return nil
}

func (i *Index) Load() error {
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("index.Load: file.Seek: %w", err)
	}
	stat, err := i.file.Stat()
	if err != nil {
		return fmt.Errorf("index.Load: file.Stat: %w", err)
	}
	b := make([]byte, stat.Size())
	n, err := i.file.Read(b)
	if err != nil {
		return fmt.Errorf("index.Load: file.Read: %w", err)
	}
	if n != len(b) {
		return NewIncompleteReadError(len(b), n)
	}
	if err = i.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("index.Load: UnmarshalBinary: %w", err)
	}
	return nil
}

func (i *Index) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeIndex); err != nil {
		return nil, fmt.Errorf("index.MarshalBinary: type: %w", err)
	}

	// length
	item := Item{}
	itemsLen := uint32(i.btree.Len()) * (item.TLVLength() + types.LenMeta)
	if err := binary.Write(&buf, binary.LittleEndian, itemsLen); err != nil {
		return nil, fmt.Errorf("index.MarshalBinary: len: %w", err)
	}

	for _, v := range i.GetAll() {
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("index.MarshalBinary: value: %w", err)
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (i *Index) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	int32Unmarshaler := encoding.NewValueUnmarshaler[uint32]()
	int64Unmarshaler := encoding.NewValueUnmarshaler[int64]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("index.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("index.UnmarshalBinary: len: %w", err)
	}
	n += types.LenInt32

	for {
		// type of index item
		if err := byteUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("index.UnmarshalBinary: ID type: %w", err)
		}
		n++
		// len of index item
		if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: ID len: %w", err)
		}
		n += types.LenInt32

		idTLV := encoding.NewTLVUnmarshaler(int64Unmarshaler)
		if err := idTLV.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: ID TLV: %w", err)
		}
		n += int(idTLV.BytesRead)
		id := idTLV.Value

		pagePosTLV := encoding.NewTLVUnmarshaler(int64Unmarshaler)
		if err := pagePosTLV.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: page pos: %w", err)
		}
		n += int(pagePosTLV.BytesRead)
		pagePos := pagePosTLV.Value
		i.Add(id, pagePos)
	}
}

func (i *Index) GetAll() []Item {
	out := make([]Item, 0)
	i.btree.Ascend(func(a Item) bool {
		out = append(out, a)
		return true
	})
	return out
}

type Item struct {
	id int64
	// PagePos is the byte position where the page starts in the table returned by os.File.Seek()
	PagePos int64
}

func NewItem(id, pagePos int64) *Item {
	return &Item{
		id:      id,
		PagePos: pagePos,
	}
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeIndexItem); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: type: %w", err)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, i.TLVLength()); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: len: %w", err)
	}
	idTLV := encoding.NewTLVMarshaler(i.id)
	idBuf, err := idTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: ID TLV: %w", err)
	}
	buf.Write(idBuf)

	pagePosTLV := encoding.NewTLVMarshaler(i.PagePos)
	pagePosBuf, err := pagePosTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: page pos: %w", err)
	}
	buf.Write(pagePosBuf)
	return buf.Bytes(), nil
}

func (i *Item) TLVLength() uint32 {
	return uint32(2*types.LenInt64 + 2*types.LenMeta)
}

// ReadRaw returns the raw byte array stored in the idx. It's for debugging
func (i *Index) ReadRaw() ([]byte, error) {
	_, err := i.file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("Index.ReadRaw: file.Seek: %w", err)
	}

	stat, err := i.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("Index.ReadRaw: file.Stat: %w", err)
	}

	buf := make([]byte, stat.Size())
	_, err = i.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("Index.ReadRaw: file.Read: %w", err)
	}

	return buf, nil
}
