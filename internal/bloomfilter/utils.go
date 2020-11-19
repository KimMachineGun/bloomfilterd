package bloomfilter

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

// Reference: https://hur.st/bloomfilter/
func getMKFromNP(n uint64, p float64) (uint64, uint64) {
	m := uint64(math.Ceil(float64(n) * math.Log(p) / math.Log(1/math.Pow(2, math.Log(2)))))
	k := uint64(math.Round(float64(m/n) * math.Log(2)))
	return m, k
}

func getMurmur3Hashers(k uint64) []hash.Hash64 {
	hashes := make([]hash.Hash64, k)
	for idx := range hashes {
		hashes[idx] = murmur3.New64WithSeed(uint32(idx))
	}
	return hashes
}
