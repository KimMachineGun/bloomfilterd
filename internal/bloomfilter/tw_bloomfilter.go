package bloomfilter

import (
	"fmt"
	"hash"
	"sync"
)

type TermWindowedBloomFilter struct {
	n uint64  // number of items
	p float64 // probability of false positives
	m uint64  // number of bits
	k uint64  // number of hash functions

	pool    *sync.Pool
	hashers []hash.Hash64

	tbfs      []*tbf
	retention uint64
	term      struct {
		latest   uint64
		earliest uint64
	}
}

type tbf struct {
	t uint64
	f *bf
}

func NewTermWindowedBloomFilter(n uint64, p float64, retention uint64) *TermWindowedBloomFilter {
	m, k := getMKFromNP(n, p)

	tbfs := make([]*tbf, retention)
	for i := range tbfs {
		tbfs[i] = &tbf{
			t: uint64(i),
			f: newBF(m),
		}
	}

	return &TermWindowedBloomFilter{
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

		tbfs:      tbfs,
		retention: retention,
	}
}

func (f *TermWindowedBloomFilter) N() uint64 {
	return f.n
}

func (f *TermWindowedBloomFilter) P() float64 {
	return f.p
}

func (f *TermWindowedBloomFilter) M() uint64 {
	return f.m
}

func (f *TermWindowedBloomFilter) K() uint64 {
	return f.k
}

func (f *TermWindowedBloomFilter) Term() uint64 {
	return f.term.latest
}

func (f *TermWindowedBloomFilter) Retention() uint64 {
	return f.retention
}

func (f *TermWindowedBloomFilter) Set(key []byte) bool {
	return f.set(key)
}

func (f *TermWindowedBloomFilter) SetWithTerm(term uint64, key []byte) (bool, error) {
	if err := f.updateTerm(term); err != nil {
		return false, err
	}

	return f.set(key), nil
}

func (f *TermWindowedBloomFilter) set(key []byte) bool {
	hashes := f.generateHashes(key)
	defer f.poolHashes(hashes)

	for i := f.term.earliest; i < f.term.latest; i++ {
		tbf := f.getTBF(i)
		if tbf.t < f.term.earliest {
			continue
		}

		if tbf.f.check(hashes) {
			return true
		}
	}

	return f.getTBF(f.term.latest).f.set(hashes)
}

func (f *TermWindowedBloomFilter) Check(key []byte) bool {
	return f.check(key)
}

func (f *TermWindowedBloomFilter) CheckWithTerm(term uint64, key []byte) (bool, error) {
	if err := f.updateTerm(term); err != nil {
		return false, err
	}

	return f.check(key), nil
}

func (f *TermWindowedBloomFilter) check(key []byte) bool {
	hashes := f.generateHashes(key)
	defer f.poolHashes(hashes)

	for i := f.term.latest; i >= f.term.earliest; i-- {
		tbf := f.getTBF(i)
		if tbf.t < f.term.earliest {
			continue
		}

		if tbf.f.check(hashes) {
			return true
		}
	}

	return false
}

func (f *TermWindowedBloomFilter) UpdateTerm(term uint64) error {
	return f.updateTerm(term)
}

type ExpiredTermError struct {
	current uint64
	given   uint64
}

func (e *ExpiredTermError) Error() string {
	return fmt.Sprintf("expired latestTerm: '%d', but given '%d'", e.current, e.given)
}

func (f *TermWindowedBloomFilter) updateTerm(term uint64) error {
	if term == f.term.latest {
		return nil
	}

	if term < f.term.latest {
		return &ExpiredTermError{
			current: f.term.latest,
			given:   term,
		}
	}

	f.term.latest = term
	if (f.retention - 1) <= f.term.latest {
		f.term.earliest = f.term.latest - (f.retention - 1)
	}

	tbf := f.getTBF(f.term.latest)
	tbf.t = f.term.latest
	tbf.f.reset()

	return nil
}

func (f *TermWindowedBloomFilter) getTBF(term uint64) *tbf {
	return f.tbfs[term%f.retention]
}

func (f *TermWindowedBloomFilter) generateHashes(key []byte) []uint64 {
	hashes := f.pool.Get().([]uint64)
	for i, h := range f.hashers {
		h.Write(key)
		hashes[i] = h.Sum64()
		h.Reset()
	}

	return hashes
}

func (f *TermWindowedBloomFilter) poolHashes(hashes []uint64) {
	f.pool.Put(hashes)
}
