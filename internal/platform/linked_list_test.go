package platform

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLinkedList_Append(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	assert.Equal(t, []int{1, 2, 3}, l.Values())
	assert.Equal(t, 3, l.Count())
}

func TestLinkedList_Find(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	node, err := l.Find(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, node.val)

	node, err = l.Find(2)
	assert.Nil(t, err)
	assert.Equal(t, 2, node.val)

	node, err = l.Find(3)
	assert.Nil(t, err)
	assert.Equal(t, 3, node.val)

	node, err = l.Find(4)
	assert.ErrorIs(t, err, &ItemNotFoundError{})
}

func TestLinkedList_FindByIdx(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	node, err := l.FindByIdx(0)
	assert.Nil(t, err)
	assert.Equal(t, 1, node.val)

	node, err = l.FindByIdx(1)
	assert.Nil(t, err)
	assert.Equal(t, 2, node.val)

	node, err = l.FindByIdx(2)
	assert.Nil(t, err)
	assert.Equal(t, 3, node.val)

	node, err = l.FindByIdx(3)
	assert.ErrorIs(t, err, &ItemNotFoundError{})
}

func TestLinkedList_RemoveHead(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	err := l.Remove(1)
	assert.Nil(t, err)
	assert.Equal(t, []int{2, 3}, l.Values())
	assert.Equal(t, 2, l.Count())
	assert.Equal(t, 2, l.head.val)
	assert.Equal(t, 3, l.tail.val)
}

func TestLinkedList_RemoveMiddle(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	err := l.Remove(2)
	assert.Nil(t, err)
	assert.Equal(t, []int{1, 3}, l.Values())
	assert.Equal(t, 2, l.Count())
	assert.Equal(t, 1, l.head.val)
	assert.Equal(t, 3, l.tail.val)
}

func TestLinkedList_RemoveTail(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	err := l.Remove(3)
	assert.Nil(t, err)
	assert.Equal(t, []int{1, 2}, l.Values())
	assert.Equal(t, 2, l.Count())
	assert.Equal(t, 1, l.head.val)
	assert.Equal(t, 2, l.tail.val)
}

func TestLinkedList_RemoveMulti(t *testing.T) {
	l := NewLinkedList[int](func(a, b int) bool { return a == b })
	l.Append(1)
	l.Append(2)
	l.Append(3)

	err := l.Remove(2)
	assert.Nil(t, err)
	assert.Equal(t, []int{1, 3}, l.Values())
	assert.Equal(t, 2, l.Count())
	assert.Equal(t, 1, l.head.val)
	assert.Equal(t, 3, l.tail.val)

	err = l.Remove(3)
	assert.Nil(t, err)
	assert.Equal(t, []int{1}, l.Values())
	assert.Equal(t, 1, l.Count())
	assert.Equal(t, 1, l.head.val)
	assert.Equal(t, 1, l.tail.val)

	err = l.Remove(1)
	assert.Nil(t, err)
	assert.Equal(t, []int{}, l.Values())
	assert.Equal(t, 0, l.Count())
	assert.Nil(t, l.head)
	assert.Nil(t, l.tail)
}
