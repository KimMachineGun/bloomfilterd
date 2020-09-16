package fsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/KimMachineGun/bfd/internal/bloomfilter"

	"github.com/hashicorp/raft"
)

type FSM struct {
	mu *sync.RWMutex
	bf *bloomfilter.BloomFilter
}

func New(n uint64, p float64) *FSM {
	return &FSM{
		mu: &sync.RWMutex{},
		bf: bloomfilter.New(n, p),
	}
}

func (m *FSM) Apply(l *raft.Log) interface{} {
	var msg Message
	err := json.Unmarshal(l.Data, &msg)
	if err != nil {
		return fmt.Errorf("cannot unmarshal message: %v", err)
	}

	switch msg.Op {
	case OpSet:
		return m.opSet(msg)
	case OpCheck:
		return m.opCheck(msg)
	}

	return fmt.Errorf("unknown operation: %v", msg.Op)
}

func (m *FSM) Snapshot() (raft.FSMSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var (
		src = m.bf.Bytes()
		dst = make([]byte, len(src))
	)
	copy(dst, src)

	return &Snapshot{
		b: dst,
	}, nil
}

func (m *FSM) Restore(rc io.ReadCloser) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := io.ReadFull(rc, m.bf.Bytes())
	return err
}

func (m *FSM) Check(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.bf.Check([]byte(key))
}

func (m *FSM) opSet(msg Message) interface{} {
	if len(msg.Args) != 1 {
		return errors.New("invalid arguments")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.bf.Set([]byte(msg.Args[0]))
}

func (m *FSM) opCheck(msg Message) interface{} {
	if len(msg.Args) != 1 {
		return errors.New("invalid arguments")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.bf.Check([]byte(msg.Args[0]))
}
