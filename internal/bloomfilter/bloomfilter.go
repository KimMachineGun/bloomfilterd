package bloomfilter

import (
	"hash"
	"sync"
)

type BloomFilter struct {
	n uint64  // number of items
	p float64 // probability of false positives
	m uint64  // number of bits
	k uint64  // number of hash functions

	pool    *sync.Pool
	hashers []hash.Hash64

	bf *bf
}

func New(n uint64, p float64) *BloomFilter {
	m, k := getMKFromNP(n, p)
	return &BloomFilter{
		n: n,
		p: p,
		m: m,
		k: k,

		pool: &sync.Pool{
			New: func() interface{} {
				return make([]uint64, k)
			},
		},
		hashers: getMurmur3Hashers(k),

		bf: newBF(m),
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
	return f.bf.b
}

func (f *BloomFilter) Set(key []byte) bool {
	hashes := f.generateHashes(key)
	defer f.poolHashes(hashes)

	return f.bf.set(hashes)
}

func (f *BloomFilter) Check(key []byte) bool {
	hashes := f.generateHashes(key)
	defer f.poolHashes(hashes)

	return f.bf.check(hashes)
}

func (f *BloomFilter) Reset() {
	f.bf.reset()
}

func (f *BloomFilter) generateHashes(key []byte) []uint64 {
	hashes := f.pool.Get().([]uint64)
	for i, h := range f.hashers {
		h.Write(key)
		hashes[i] = h.Sum64()
		h.Reset()
	}

	return hashes
}

func (f *BloomFilter) poolHashes(hashes []uint64) {
	f.pool.Put(hashes)
}
