package platform

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLRU_Put(t *testing.T) {
	equals := func(a, b string) bool { return a == b }
	lru := NewLRU[string, int](3, equals)

	err := lru.Put("first", 1)
	assert.Nil(t, err)
	err = lru.Put("second", 2)
	assert.Nil(t, err)
	err = lru.Put("third", 3)
	assert.Nil(t, err)

	assert.Equal(t, 3, lru.len)
	assert.Equal(t, 1, lru.hMap["first"])
	assert.Equal(t, 2, lru.hMap["second"])
	assert.Equal(t, 3, lru.hMap["third"])

	assertList(t, lru, []string{"first", "second", "third"})
}

func TestLRU_PutCapacityExceeded(t *testing.T) {
	equals := func(a, b string) bool { return a == b }
	lru := NewLRU[string, int](3, equals)

	err := lru.Put("first", 1)
	assert.Nil(t, err)
	err = lru.Put("second", 2)
	assert.Nil(t, err)
	err = lru.Put("third", 3)
	assert.Nil(t, err)
	err = lru.Put("fourth", 4)
	assert.Nil(t, err)

	assert.Equal(t, 3, lru.len)
	assert.Equal(t, 2, lru.hMap["second"])
	assert.Equal(t, 3, lru.hMap["third"])
	assert.Equal(t, 4, lru.hMap["fourth"])

	assertList(t, lru, []string{"second", "third", "fourth"})
}

func TestLRU_Get(t *testing.T) {
	equals := func(a, b string) bool { return a == b }
	lru := NewLRU[string, int](3, equals)

	err := lru.Put("first", 1)
	assert.Nil(t, err)
	err = lru.Put("second", 2)
	assert.Nil(t, err)
	err = lru.Put("third", 3)
	assert.Nil(t, err)

	val, err := lru.Get("first")
	assert.Equal(t, 1, val)
	assertList(t, lru, []string{"second", "third", "first"})

	val, err = lru.Get("first")
	assert.Equal(t, 1, val)
	assertList(t, lru, []string{"second", "third", "first"})

	val, err = lru.Get("third")
	assert.Equal(t, 3, val)
	assertList(t, lru, []string{"second", "first", "third"})

	val, err = lru.Get("second")
	assert.Equal(t, 2, val)
	assertList(t, lru, []string{"first", "third", "second"})

	val, err = lru.Get("third")
	assert.Equal(t, 3, val)
	assertList(t, lru, []string{"first", "second", "third"})
}

func assertList(t *testing.T, lru *LRU[string, int], expected []string) {
	values := make([]string, 0)
	for _, v := range lru.list.Values() {
		values = append(values, v)
	}
	assert.Equal(t, expected, values)
}
