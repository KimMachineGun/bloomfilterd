package bloomfilter

import (
	"math"
)

type bf struct {
	m uint64 // number of bits
	b []byte
}

func newBF(m uint64) *bf {
	return &bf{
		m: m,
		b: make([]byte, int(math.Ceil(float64(m)/float64(8)))),
	}
}

func (f *bf) set(hashes []uint64) bool {
	exists := true
	for _, h := range hashes {
		exists = f.setBit(h) && exists
	}

	return exists
}

func (f *bf) check(hashes []uint64) bool {
	for _, h := range hashes {
		bytesIdx, idx := f.getIndices(h)
		b := f.b[bytesIdx]
		if b&(1<<idx) == 0 {
			return false
		}
	}

	return true
}

func (f *bf) reset() {
	for i := range f.b {
		f.b[i] = 0
	}
}

func (f *bf) setBit(hash uint64) bool {
	bytesIdx, idx := f.getIndices(hash)
	b := f.b[bytesIdx]
	exists := b&(1<<idx) != 0
	if !exists {
		f.b[bytesIdx] = b | (1 << idx)
	}

	return exists
}

func (f *bf) getIndices(hash uint64) (uint64, uint64) {
	idx := hash % f.m
	return uint64(len(f.b)-1) - idx/8, idx % 8
}
