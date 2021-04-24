package main

import (
	"errors"
	"time"
)

var (
	DefaultConfig = Config{
		N: 100000,
		P: 0.000001,
		InitialMembers: map[uint64]string{
			1: "localhost:63001",
			2: "localhost:63002",
			3: "localhost:63003",
		},
		Timeout: 500 * time.Millisecond,
	}
)

type Config struct {
	N uint    `flago:"n,number of items in the filter"`
	P float64 `flago:"p,probability of false positives, fraction between 0 and 1"`

	NodeID         uint64  `flago:"id,node id"`
	Addr           string  `flago:"addr,address of the node"`
	Join           bool    `flago:"join,join the cluster"`
	InitialMembers MapFlag `flago:"members,initial members of raft cluster"`

	Timeout time.Duration `flago:"timeout,timeout for raft operations"`
}

func (c *Config) Validate() error {
	if c.N == 0 {
		return errors.New("invalid N")
	}
	if c.P == 0 {
		return errors.New("invalid P")
	}

	if c.NodeID == 0 {
		return errors.New("invalid NodeID")
	}
	if c.Addr == "" {
		return errors.New("invalid Addr")
	}
	if len(c.InitialMembers) == 0 {
		return errors.New("invalid InitialMembers")
	}

	if c.Timeout < time.Millisecond {
		return errors.New("invalid Timeout")
	}

	return nil
}
