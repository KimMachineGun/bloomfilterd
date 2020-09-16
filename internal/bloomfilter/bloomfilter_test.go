package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	a := assert.New(t)

	bf := New(4000, 1e-7)
	a.NotNil(bf)

	res := bf.Set([]byte("key1"))
	a.False(res)

	res = bf.Set([]byte("key1"))
	a.True(res)

	res = bf.Check([]byte("key1"))
	a.True(res)

	res = bf.Check([]byte("key2"))
	a.False(res)

	res = bf.Set([]byte("key2"))
	a.False(res)

	res = bf.Set([]byte("key2"))
	a.True(res)

	res = bf.Check([]byte("key2"))
	a.True(res)
}
