package bloomfilter

import (
	"hash"
	"math"
)

type BloomFilter struct {
	n uint64  // number of items
	p float64 // probability of false positives
	m uint64  // number of bits
	k uint64  // number of hash functions

	bytes  []byte
	hashes []hash.Hash64
}

func New(n uint64, p float64) *BloomFilter {
	m, k := getMKFromNP(n, p)
	return &BloomFilter{
		n: n,
		p: p,
		m: m,
		k: k,

		bytes:  make([]byte, int(math.Ceil(float64(m)/float64(8)))),
		hashes: getMurMur3Hashes(k),
	}
}

func (f *BloomFilter) N() uint64 {
	return f.n
}

func (f *BloomFilter) P() float64 {
	return f.p
}

func (f *BloomFilter) M() uint64 {
	return f.m
}

func (f *BloomFilter) K() uint64 {
	return f.k
}

func (f *BloomFilter) Bytes() []byte {
	return f.bytes
}

func (f *BloomFilter) Set(key []byte) bool {
	exists := true
	for _, h := range f.hashes {
		exists = f.setBit(f.getIndices(key, h)) && exists
	}

	return exists
}

func (f *BloomFilter) Check(key []byte) bool {
	for _, h := range f.hashes {
		bytesIdx, idx := f.getIndices(key, h)
		b := f.bytes[bytesIdx]
		if b&(1<<idx) == 0 {
			return false
		}
	}

	return true
}

func (f *BloomFilter) setBit(bytesIdx uint64, idx uint64) bool {
	b := f.bytes[bytesIdx]
	exists := b&(1<<idx) != 0
	if !exists {
		f.bytes[bytesIdx] = b | (1 << idx)
	}

	return exists
}

func (f *BloomFilter) getIndices(key []byte, h hash.Hash64) (uint64, uint64) {
	h.Write(key)
	defer h.Reset()
	idx := h.Sum64() % f.m
	return uint64(len(f.bytes)-1) - idx/8, idx % 8
}
