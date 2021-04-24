package bsm

import (
	"io"

	"github.com/bits-and-blooms/bloom/v3"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

type Builder struct {
	N uint
	P float64
}

func NewBuilder(n uint, p float64) *Builder {
	return &Builder{N: n, P: p}
}

func (b *Builder) BuildBSM(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &BSM{
		N: b.N,
		P: b.P,

		ClusterID: clusterID,
		NodeID:    nodeID,
		Filter:    bloom.NewWithEstimates(b.N, b.P),
	}
}

type BSM struct {
	N uint
	P float64

	ClusterID uint64
	NodeID    uint64
	Filter    *bloom.BloomFilter
}

func (m *BSM) Update(b []byte) (sm.Result, error) {
	present := m.Filter.TestAndAdd(b)
	if present {
		return sm.Result{Value: 1}, nil
	}
	return sm.Result{Value: 0}, nil
}

func (m *BSM) Lookup(v interface{}) (interface{}, error) {
	b, _ := v.([]byte)
	present := m.Filter.Test(b)
	if present {
		return sm.Result{Value: 1}, nil
	}
	return sm.Result{Value: 0}, nil
}

func (m *BSM) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, stop <-chan struct{}) error {
	_, err := m.Filter.WriteTo(w)
	if err != nil {
		return err
	}
	return nil
}

func (m *BSM) RecoverFromSnapshot(w io.Reader, ss []sm.SnapshotFile, stop <-chan struct{}) error {
	_, err := m.Filter.ReadFrom(w)
	if err != nil {
		return err
	}
	return nil
}

func (m *BSM) Close() error {
	return nil
}
