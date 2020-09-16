package fsm

import (
	"github.com/hashicorp/raft"
)

type Snapshot struct {
	b []byte
}

func (s Snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.b)
	if err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s Snapshot) Release() {}
