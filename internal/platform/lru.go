package platform

import (
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type LRU[K types.Scalar, V any] struct {
	list *LinkedList[K]
	hMap map[K]V
	cap  int
	len  int
}

func NewLRU[K types.Scalar, V any](cap int, equals EqFunc[K]) *LRU[K, V] {
	return &LRU[K, V]{
		list: NewLinkedList[K](equals),
		cap:  cap,
		hMap: make(map[K]V),
	}
}

func (lru *LRU[K, V]) Get(key K) (V, error) {
	var zero V
	val, ok := lru.hMap[key]
	if !ok {
		return zero, NewItemNotFoundError(lru.hMap, key)
	}
	err := lru.list.Remove(key)
	if err != nil {
		return zero, fmt.Errorf("lru.Get: %w", err)
	}
	lru.list.Append(key)
	return val, nil
}

func (lru *LRU[K, V]) Put(key K, val V) error {
	if lru.len >= lru.cap {
		if err := lru.removeLeastRecentlyUsed(); err != nil {
			return fmt.Errorf("lru.Put: %w", err)
		}
	}
	lru.list.Append(key)
	lru.hMap[key] = val
	lru.len++
	return nil
}

func (lru *LRU[K, V]) Remove(key K) error {
	_, ok := lru.hMap[key]
	if !ok {
		return NewItemNotFoundError(lru.hMap, key)
	}
	delete(lru.hMap, key)
	return lru.list.Remove(key)
}

func (lru *LRU[T, K]) removeLeastRecentlyUsed() error {
	node, err := lru.list.RemoveByIdx(0)
	if err != nil {
		return fmt.Errorf("lru.removeLeastRecentlyUsed: %w", err)
	}
	lru.len--
	delete(lru.hMap, node.val)
	return nil
}
