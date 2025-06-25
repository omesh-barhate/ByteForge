package fulltext

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIndex_Add(t *testing.T) {
	f := createFile()
	defer removeFile()

	idx := NewIndex(f)
	idx.Add("software", 100, 10)
	idx.Add("engineer", 100, 10)
	idx.Add("engineer", 200, 20)

	val, err := idx.Get("software")
	assert.Nil(t, err)
	assert.Equal(t, val, []*IndexItem{NewIndexItem(100, 10)})

	val, err = idx.Get("engineer")
	assert.Nil(t, err)
	assert.Equal(t, val, []*IndexItem{NewIndexItem(100, 10), NewIndexItem(200, 20)})

	val, err = idx.Get("nope")
	assert.NotNil(t, err)
	assert.Nil(t, val)
}

func TestIndex_AddEmptyKey(t *testing.T) {
	f := createFile()
	defer removeFile()

	idx := NewIndex(f)
	idx.Add("", 100, 10)

	_, err := idx.Get("")
	assert.NotNil(t, err)
	assert.Len(t, idx.hMap, 0)
}

func TestIndex_RemoveMany(t *testing.T) {
	f := createFile()
	defer removeFile()

	idx := NewIndex(f)
	idx.Add("software", 100, 10)
	idx.Add("engineer", 100, 20)
	idx.Add("engineer", 200, 30)

	idx.RemoveMany([]int64{20, 30})

	val, err := idx.Get("software")
	assert.Nil(t, err)
	assert.Equal(t, val, []*IndexItem{NewIndexItem(100, 10)})

	val, err = idx.Get("engineer")
	assert.NotNil(t, err)
	assert.Nil(t, val)
}

func createFile() *os.File {
	f, err := os.Create("idx.bin")
	if err != nil {
		panic(err)
	}
	return f
}

func removeFile() {
	if err := os.Remove("idx.bin"); err != nil {
		panic(err)
	}
}
