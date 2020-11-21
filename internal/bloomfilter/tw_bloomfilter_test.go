package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	a := assert.New(t)

	twbf := NewTermWindowedBloomFilter(4000, 1e-7, 3)
	a.NotNil(twbf)

	a.False(twbf.Set([]byte("key1")))
	a.True(twbf.Check([]byte("key1")))

	a.NoError(twbf.UpdateTerm(1))
	a.True(twbf.Check([]byte("key1")))
	a.False(twbf.Set([]byte("key2")))

	a.NoError(twbf.UpdateTerm(2))
	a.True(twbf.Check([]byte("key1")))
	a.True(twbf.Check([]byte("key2")))

	ok, err := twbf.SetWithTerm(3, []byte("key3"))
	a.NoError(err)
	a.False(ok)
	a.False(twbf.Check([]byte("key1")))
	a.True(twbf.Check([]byte("key2")))
	a.True(twbf.Check([]byte("key3")))

	a.NoError(twbf.UpdateTerm(4))
	a.False(twbf.Check([]byte("key1")))
	a.False(twbf.Check([]byte("key2")))
	a.True(twbf.Check([]byte("key3")))

	a.NoError(twbf.UpdateTerm(5))
	a.False(twbf.Check([]byte("key1")))
	a.False(twbf.Check([]byte("key2")))
	a.True(twbf.Check([]byte("key3")))

	a.NoError(twbf.UpdateTerm(6))
	a.False(twbf.Check([]byte("key1")))
	a.False(twbf.Check([]byte("key2")))
	a.False(twbf.Check([]byte("key3")))
}
