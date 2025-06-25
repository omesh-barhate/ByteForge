package bytes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTrimZeroBytes(t *testing.T) {
	b := []byte{1, 2, 3, 0, 0, 0}
	trimmed := TrimZeroBytes(b)
	assert.Equal(t, []byte{1, 2, 3}, trimmed)
}

func TestTrimZeroBytes_EmptySlice(t *testing.T) {
	b := []byte{}
	trimmed := TrimZeroBytes(b)
	assert.Equal(t, []byte{}, trimmed)
}
